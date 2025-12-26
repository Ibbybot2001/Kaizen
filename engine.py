from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from schema import MarketState, Hypothesis, StructureEvent, EventType, Regime, Session, Direction

class HypothesisEngine:
    """
    Deterministic execution engine.
    Scans the MarketState graph and tests a specific Hypothesis.
    """
    
    def __init__(self, states: Dict[datetime, MarketState], df: pd.DataFrame):
        self.states = states
        # Convert df to dictionary for O(1) price lookup by timestamp if needed, 
        # or just rely on state if it carries price. 
        # Currently State doesn't carry OHLC explicitly, only context.
        # We need OHLC to check outcomes (Expansion / Invalidation).
        
        self.price_lookup = df.set_index('time')[['open', 'high', 'low', 'close', 'atr']].to_dict('index')
        self.sorted_times = sorted(states.keys())
        
        # Build map from time -> index for fast forward scanning
        self.time_to_idx = {t: i for i, t in enumerate(self.sorted_times)}

    def run(self, hypothesis: Hypothesis, mode: str = "NORMAL", random_seed: int = 42) -> pd.DataFrame:
        """
        Scans all states for Trigger + Conditions.
        mode: "NORMAL" or "NULL_RANDOM_DIRECTION"
        """
        import random
        random.seed(random_seed)
        
        results = []
        
        print(f"Testing Hypothesis {hypothesis.id} | Mode: {mode}")
        
        for t in self.sorted_times:
            state = self.states[t]
            
            # 1. Check Pre-Conditions
            if not self._check_conditions(state, hypothesis.conditions):
                continue
            
            # 2. Check Trigger
            trigger_event = None
            for e in state.recent_events:
                if e.event_type == hypothesis.trigger.event_type:
                    if e.end_bar == t: 
                        # Check Trigger Conditions (Specific to the Event)
                        # e.g. is_major == True
                        if self._check_trigger_conditions(e, hypothesis.trigger.conditions):
                            trigger_event = e
                            break
            
            if not trigger_event:
                continue
                
            # Determine Direction based on Mode
            if mode == "NORMAL":
                # Strategy logic defines direction (Sweep Low -> Bullish)
                # Usually part of the Trigger Event or Hypothesis logic
                # Our Trigger Event HAS a direction already assigned by Extractor.
                trade_direction = trigger_event.direction
            elif mode == "NULL_RANDOM_DIRECTION":
                # Override with coin flip
                trade_direction = random.choice([Direction.BULLISH, Direction.BEARISH])
                
            # 3. Simulate Outcome
            outcome = self._simulate_outcome(t, hypothesis, trigger_event, trade_direction)
            if outcome:
                outcome['trigger_time'] = t
                outcome['session'] = state.session.value if hasattr(state.session, 'value') else state.session
                outcome['regime'] = state.regime.value if hasattr(state.regime, 'value') else state.regime
                outcome['mode'] = mode
                
                # Enhanced Context for Loss Analysis
                if trigger_event.event_type == EventType.LIQUIDITY_SWEEP:
                    outcome['sweep_depth'] = getattr(trigger_event, 'sweep_depth', 0)
                    outcome['is_major'] = getattr(trigger_event, 'is_major', False)
                else:
                    outcome['sweep_depth'] = 0
                    outcome['is_major'] = False
                    
                # Context Tags
                if trigger_event.context:
                    outcome['vwap_dist'] = trigger_event.context.distance_to_vwap_std
                    outcome['time_of_day'] = trigger_event.context.time_of_day
                    
                results.append(outcome)
                
        return pd.DataFrame(results)

    def _check_conditions(self, state: MarketState, conditions: List[Any]) -> bool:
        # ... (Unchanged)
        for cond in conditions:
            if cond.metric == 'session':
                if state.session != cond.value: return False
            elif cond.metric == 'regime':
                if state.regime != cond.value: return False
            elif cond.metric == 'price_relation_to_vwap':
                if cond.operator == '==':
                    if state.price_relation_to_vwap != cond.value: return False
        return True

    def _check_trigger_conditions(self, event: StructureEvent, conditions: List[Any]) -> bool:
        """
        Validates conditions against the Trigger Event itself.
        """
        for cond in conditions:
            # Example: metric="is_major", value=True
            if hasattr(event, cond.metric):
                val = getattr(event, cond.metric)
                if cond.operator == '==':
                    if val != cond.value: return False
                # Add other operators if needed
        return True

    def _simulate_outcome(self, start_time: datetime, hypothesis: Hypothesis, trigger: StructureEvent, direction: Direction) -> Optional[Dict]:
        """
        Forward tests from start_time.
        direction: The explicit direction to trade (Standard or Random).
        """
        start_idx = self.time_to_idx[start_time]
        
        # Initial params
        entry_price = self.price_lookup[start_time]['close']
        atr = self.price_lookup[start_time]['atr']
        
        # Risk Calculation
        # We need to know the 'Structural Stop Distance' intended by the strategy
        # even if we are trading the other way (to keep "Same Invalidation Logic" / Risk Sizing).
        
        # Original Strategy Logic for Stp
        # If Sweep Low (Bullish Trigger): Stop is Low of Sweep. Distance = Entry - Low.
        # If Sweep High (Bearish Trigger): Stop is High of Sweep. Distance = High - Entry.
        
        stop_distance = 0.0
        
        if trigger.event_type == EventType.LIQUIDITY_SWEEP:
            # Calculate the structural risk distance based on the EVENT (not necessarily the trade direction)
            # Example: A sweep of a low happens. The "structure" implies the Low is important.
            # Implied Bullish Stop = Entry - Low
            # Implied Bearish Stop (Breakout) = High - Entry? Or same distance?
            # User Protocol: "Use same SL / invalidation logic".
            # This implies symmetric risk distance if we flip direction.
            
            sweep_high = self.price_lookup[start_time]['high']
            sweep_low = self.price_lookup[start_time]['low']
            
            if trigger.direction == Direction.BULLISH: # Original Event was Bullish (Sweep Low)
                stop_distance = entry_price - sweep_low
            else: # Original Event was Bearish (Sweep High)
                stop_distance = sweep_high - entry_price
        else:
            # Default fallback (1 ATR?)
            stop_distance = 1.0 * atr
            
        # Ensure non-zero positive distance
        if stop_distance <= 0: stop_distance = 0.5 * atr
        
        # Calculate Targets based on TRADE Direction
        is_long = (direction == Direction.BULLISH)
        
        # STOP PRICE
        stop_price = (entry_price - stop_distance) if is_long else (entry_price + stop_distance)
        
        # TARGET PRICE (3R)
        # Using the SAME R-multiple as strategy
        target_dist = hypothesis.expectation.min_value * stop_distance # Use structural Risk as R unit? 
        # Wait, strategy definition says "3.0 * ATR" or just "3.0" R?
        # Hypothesis expectation says 'target_metric="r_multiple"'. 
        # Usually R is defined by the risk.
        # But previous runs used 'target_dist = min_value * atr'.
        # Let's align. If min_value is "3.0" and target_metric is "r_multiple", it means 3 * Risk.
        # But if code previously calculated based on ATR...
        # Code check: `target_dist = hypothesis.expectation.min_value * atr`
        # This implies Fixed ATR Risk in previous run?
        # Let's check previous code snippet (Step 365).
        # Yes, it used `min_value * atr`.
        # AND it used `inval_price` based on candle Low/High.
        # So Risk = (Entry - High/Low)? No, risk was dynamic.
        # Target was Fixed ATR?
        # If Target is Fixed ATR, then R-multiple varies per trade.
        # The User report said "27.4% win at 3R". 
        # If Target was ATR-based, was Risk also ATR-based?
        # Let's check invalidation again.
        # `inval_price = ... low/high`.
        # So Risk IS dynamic (candle size).
        # Target was `hypothesis.expectation.min_value * atr`.
        # So Reward is ATR-based. Risk is Candle-based.
        # This means R is NOT fixed 3.0 outcome. It's var-R.
        # BUT... `pnl_r` calculation in result?
        # Previous code:
        # `pnl_r`: hypothesis.expectation.min_value` (if win) or `-1.0` (if loss).
        # This HARDCODES the result R value, assuming the setup matched the ratio.
        # NOTE: This effectively assumes we Position Size for 1R = Risk.
        # So if we win, we confirm we hit the target.
        # Valid Null Test MUST maintain this "Fixed R Outcome" assumption logic if we want comparable distributions.
        
        target_price = entry_price + target_dist if is_long else entry_price - target_dist
        
        if trigger.event_type == EventType.LIQUIDITY_SWEEP:
            # For a sweep, the invalidation point is the extreme of the sweep.
            # Bullish Sweep (Dir=BULLISH): We swept a Low. Extreme is swept_level - sweep_depth.
            # Bearish Sweep (Dir=BEARISH): We swept a High. Extreme is swept_level + sweep_depth.
            # (Or simply the High/Low of the trigger bar, if the sweep happened on that bar).
            # Our extraction logic says start_bar=end_bar=curr_time.
            # And sweep_depth is calculated from that bar.
            # So looking up the bar High/Low is safer/simpler than reconstructing from depth.
            if is_long:
                inval_price = self.price_lookup[start_time]['low'] 
            else:
                inval_price = self.price_lookup[start_time]['high']
        else:
            # Default for other setups
            inval_price = self.price_lookup[start_time]['low'] if is_long else self.price_lookup[start_time]['high']
        
        # Scan forward
        max_duration = hypothesis.expectation.within_bars
        
        for i in range(1, max_duration + 1):
            curr_idx = start_idx + i
            if curr_idx >= len(self.sorted_times):
                break
            
            curr_time = self.sorted_times[curr_idx]
            bar = self.price_lookup[curr_time]
            
            # Check Invalidation / Stop
            if is_long:
                if bar['close'] < stop_price: # Stop hit
                    return {
                        "result": "LOSS",
                        "pnl_r": -1.0, 
                        "bars_held": i,
                        "exit_reason": "INVALIDATION"
                    }
                # Check Target
                if bar['high'] >= target_price:
                    return {
                        "result": "WIN",
                        "pnl_r": hypothesis.expectation.min_value,
                        "bars_held": i,
                        "exit_reason": "TARGET_MET"
                    }
            else: # Short
                if bar['close'] > stop_price:
                    return {
                        "result": "LOSS",
                        "pnl_r": -1.0,
                        "bars_held": i,
                        "exit_reason": "INVALIDATION"
                    }
                if bar['low'] <= target_price:
                    return {
                        "result": "WIN",
                        "pnl_r": hypothesis.expectation.min_value,
                        "bars_held": i,
                        "exit_reason": "TARGET_MET"
                    }
        
        # If time runs out
        return {
            "result": "TIMEOUT",
            "pnl_r": 0.0, # Or actual floating PnL
            "bars_held": max_duration,
            "exit_reason": "TIME_EXPIRED"
        }

if __name__ == "__main__":
    # Integration Test
    from data_loader import DataLoader
    from structure import StructureExtractor
    from state_graph import StateBuilder
    from schema import Condition, Trigger, ResultExpectation, InvalidationCriteria
    
    print("Loading Data (Full Verification)...")
    loader = DataLoader(r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv")
    df = loader.load_and_process() # Full Data
    loader.log_missing_bars()
    
    # ... (previous setup)
    print("Extracting Structure...")
    extractor = StructureExtractor(df)
    events = []
    swings = extractor.extract_swings() # Need reference to swings for sweep extraction
    events.extend(swings)
    events.extend(extractor.extract_compressions())
    events.extend(extractor.extract_displacements())
    events.extend(extractor.extract_sweeps(swings, min_reclaim_pts=0.25)) # Add Sweeps
    
    print("Building States...")
    builder = StateBuilder(df, events)
    states = builder.build_states()
    
    # Import Kaizen Reversal Hypothesis
    from hypotheses.kaizen_reversal import get_kaizen_reversal_hypothesis
    h_kaizen = get_kaizen_reversal_hypothesis()
    
    # --- MAJOR FILTER APPLICATION ---
    # Add Condition to Trigger: is_major == True
    h_kaizen.trigger.conditions.append(
        Condition(metric="is_major", operator="==", value=True)
    )
    print("Applied Filter: Trigger.is_major == True")
    
    engine = HypothesisEngine(states, df)
    
    # 1. Real Strategy Run (Major Only)
    print("\n--- [RUN 1] Kaizen Reversal (Major Only) ---")
    results_strat = engine.run(h_kaizen, mode="NORMAL")
    results_strat.to_csv("logs/run_strategy_major.csv", index=False)
    
    if not results_strat.empty:
        win_rate = len(results_strat[results_strat['result']=='WIN']) / len(results_strat)
        mean_r = results_strat['pnl_r'].mean()
        print(f"Strategy Expectancy: {mean_r:.4f} R | WR: {win_rate*100:.1f}%")
        
    # 2. Null Hypothesis Run (on Major Only events)
    print("\n--- [RUN 2] Null Hypothesis (Random Direction, Major Filters) ---")
    results_null = engine.run(h_kaizen, mode="NULL_RANDOM_DIRECTION", random_seed=777)
    results_null.to_csv("logs/run_null_major.csv", index=False)
    
    if not results_null.empty:
        win_rate = len(results_null[results_null['result']=='WIN']) / len(results_null)
        mean_r = results_null['pnl_r'].mean()
        print(f"Null Expectancy: {mean_r:.4f} R | WR: {win_rate*100:.1f}%")

    # Comparison
    if not results_strat.empty and not results_null.empty:
        diff = results_strat['pnl_r'].mean() - results_null['pnl_r'].mean()
        print(f"Strategy Advantage: {diff:.4f} R")
        if diff > 0.05:
            print("PASS: Strategy outperforms Null significantly.")
        else:
            print("FAIL: Strategy provides no significant edge over coin flip.")
