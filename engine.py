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

    def run(self, hypothesis: Hypothesis) -> pd.DataFrame:
        """
        Scans all states for Trigger + Conditions.
        If triggered, simulates the outcome.
        Returns a DataFrame of results (The Ledger).
        """
        results = []
        
        print(f"Testing Hypothesis {hypothesis.id}: {hypothesis.description}")
        
        # Iterate through all states
        for t in self.sorted_times:
            state = self.states[t]
            
            # 1. Check Pre-Conditions
            if not self._check_conditions(state, hypothesis.conditions):
                continue
            
            # 2. Check Trigger
            # The Trigger is usually an Event that JUST happened or is happening.
            # In our schema, 'trigger' is an EventType.
            # We check if 'recent_events' contains this event ending NOW?
            # Or if the current bar represents this event?
            
            # Simplified: Check if any event in recent_events START_BAR is this bar? 
            # Or END_BAR? Usually we trade on CONFIRMATION (End Bar).
            
            trigger_event = None
            # Scan recent events (last 1-2 bars) that match trigger type
            # We need to ensure we don't double count.
            # Let's say we fire on the CLOSE of the bar where the event is confirmed.
            
            for e in state.recent_events:
                if e.event_type == hypothesis.trigger.event_type:
                    # Is this the first time we see it? 
                    # Assuming we iterate in order, we fire when current_time == e.end_bar
                    if e.end_bar == t: 
                        trigger_event = e
                        break
            
            if not trigger_event:
                continue
                
            # 3. Simulate Outcome
            outcome = self._simulate_outcome(t, hypothesis, trigger_event)
            if outcome:
                outcome['trigger_time'] = t
                results.append(outcome)
                
        return pd.DataFrame(results)

    def _check_conditions(self, state: MarketState, conditions: List[Any]) -> bool:
        """
        Validates generic conditions against the State.
        """
        for cond in conditions:
            # Example: "regime" == "LOW_VOL"
            # Example: "session" == "NY_AM"
            # Example: "price_relation_to_vwap" == "BELOW"
            # Example: "active_swings" contains "SWING_HIGH" (Not implemented generic parsing deeply yet)
            
            # Hardcoded parsing for the specific requested hypothesis style
            if cond.metric == 'session':
                if state.session != cond.value: return False
            elif cond.metric == 'regime':
                if state.regime != cond.value: return False
            elif cond.metric == 'price_relation_to_vwap':
                if cond.operator == '==':
                    if state.price_relation_to_vwap != cond.value: return False
            # Add more parsing logic here
            
        return True

    def _simulate_outcome(self, start_time: datetime, hypothesis: Hypothesis, trigger: StructureEvent) -> Optional[Dict]:
        """
        Forward tests from start_time until Expectation met or Invalidation hit.
        """
        start_idx = self.time_to_idx[start_time]
        
        # Initial params
        entry_price = self.price_lookup[start_time]['close']
        atr = self.price_lookup[start_time]['atr']
        
        target_dist = hypothesis.expectation.min_value * atr
        # Adjust target based on direction
        # If trigger direction is BULLISH -> Target is Up
        is_long = (trigger.direction == Direction.BULLISH)
        
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
            
            # Check Invalidation First (conservative)
            if is_long:
                if bar['close'] < inval_price: # Stop hit
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
                if bar['close'] > inval_price:
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
    
    print("Loading Data...")
    loader = DataLoader(r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv")
    df = loader.load_and_process().head(10000)
    
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
    
    engine = HypothesisEngine(states, df)
    results = engine.run(h_kaizen)
    
    print("\n--- Kaizen Reversal Backtest Results ---")
    if not results.empty:
        print(results['result'].value_counts())
        print(f"Total Trades: {len(results)}")
        win_rate = len(results[results['result']=='WIN']) / len(results) * 100
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Expectancy (R): { (win_rate/100 * h_kaizen.expectation.min_value) - ((100-win_rate)/100 * 1.0) :.2f}R")
        print(results.head())
    else:
        print("No trades found matching Kaizen Reversal.")
