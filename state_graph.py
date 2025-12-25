from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime, timedelta
from schema import MarketState, StructureEvent, SwingEvent, EventType, Regime, Session, Direction

class StateBuilder:
    """
    Constructs the sequence of MarketStates from raw data and extracted events.
    This creates the "Graph" that the hypothesis engine traverses.
    """
    
    def __init__(self, df: pd.DataFrame, events: List[StructureEvent]):
        self.df = df.sort_values('time').reset_index(drop=True)
        self.events = sorted(events, key=lambda x: x.start_bar)
        
        # Index events by start time for fast lookup during iteration
        self.events_by_time = {}
        for e in self.events:
            t = e.start_bar
            if t not in self.events_by_time:
                self.events_by_time[t] = []
            self.events_by_time[t].append(e)

    def build_states(self) -> Dict[datetime, MarketState]:
        """
        Iterates through the dataframe and builds a MarketState for every bar.
        Returns a dictionary for O(1) lookup by timestamp.
        """
        states = {}
        
        # State tracking variables
        active_swings: List[SwingEvent] = [] 
        recent_events_log: List[StructureEvent] = [] # Keep last N
        
        # Iterate
        # Note: In a production engine, this loop is the bottleneck. 
        # For research on 1m bars (100k+ rows), it might take a few seconds in Python.
        # Acceptable for now.
        
        print(f"Building states for {len(self.df)} bars...")
        
        for idx, row in self.df.iterrows():
            curr_time = row['time']
            curr_close = row['close']
            curr_session = Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER
            
            # 1. Update Active Swings (Invalidation Logic)
            # A Swing High is invalidated if Price closes > Swing Price (conceptually)
            # A Swing Low is invalidated if Price closes < Swing Price
            # We keep only 'Active' structure.
            
            # Filter active swings
            next_active_swings = []
            for s in active_swings:
                if s.event_type == EventType.SWING_HIGH:
                    if curr_close <= s.price_level: # Still valid
                        next_active_swings.append(s)
                elif s.event_type == EventType.SWING_LOW:
                    if curr_close >= s.price_level: # Still valid
                        next_active_swings.append(s)
            
            active_swings = next_active_swings
            
            # 2. Ingest New Events occurring AT this bar
            if curr_time in self.events_by_time:
                new_events = self.events_by_time[curr_time]
                for e in new_events:
                    # Add to Log
                    recent_events_log.append(e)
                    
                    # If it's a swing, add to active structure
                    if isinstance(e, SwingEvent):
                        active_swings.append(e)
            
            # Clean Active Swings (Keep only nearest/relevant? Or all?)
            # For now, keep all validated ones. 
            # Optimization: Only keep last 20?
            if len(active_swings) > 50:
                active_swings = active_swings[-50:]
                
            # Maintain Recent Events Log (e.g. last 20 items)
            if len(recent_events_log) > 20:
                recent_events_log = recent_events_log[-20:]
                
            # 3. Determine Regime
            # Simple heuristic for now using ATR or recent compression
            # (Ideally, Regime is its own EventType, but let's derive it)
            regime = Regime.CHOP
            if any(e.event_type == EventType.DISPLACEMENT for e in recent_events_log[-3:]):
                regime = Regime.EXPANSION
            elif any(e.event_type == EventType.COMPRESSION for e in recent_events_log[-3:]):
                regime = Regime.LOW_VOL
                
            # 4. Nearest Support/Resistance (Simple scan of active swings)
            # Resistance: Lowest Swing High above current price
            # Support: Highest Swing Low below current price
            
            resistances = [s.price_level for s in active_swings if s.event_type == EventType.SWING_HIGH and s.price_level > curr_close]
            supports = [s.price_level for s in active_swings if s.event_type == EventType.SWING_LOW and s.price_level < curr_close]
            
            nearest_resistance = min(resistances) if resistances else None
            nearest_support = max(supports) if supports else None
            
            # 5. VWAP Relation
            vwap_dist = "TOUCHING"
            if row['vwap'] and row['atr'] > 0:
                dist = (curr_close - row['vwap']) / row['atr']
                if dist > 0.5: vwap_dist = "ABOVE"
                elif dist < -0.5: vwap_dist = "BELOW"
            
            # Construct State Node
            state = MarketState(
                timestamp=curr_time,
                session=curr_session,
                regime=regime,
                active_swings=active_swings[-10:], # Just store references to last 10 relevant
                recent_events=recent_events_log,
                price_relation_to_vwap=vwap_dist,
                nearest_support=nearest_support,
                nearest_resistance=nearest_resistance
            )
            
            states[curr_time] = state
            
        print("State construction complete.")
        return states

if __name__ == "__main__":
    from data_loader import DataLoader
    from structure import StructureExtractor
    
    loader = DataLoader(r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv")
    df = loader.load_and_process()
    subset = df.head(2000) # Short test
    
    # Extract
    extractor = StructureExtractor(subset)
    events = []
    events.extend(extractor.extract_swings())
    events.extend(extractor.extract_compressions())
    events.extend(extractor.extract_displacements())
    
    # Build State
    builder = StateBuilder(subset, events)
    states = builder.build_states()
    
    # Verify a random state
    sample_time = subset.iloc[-1]['time']
    if sample_time in states:
        print(f"State at {sample_time}:")
        s = states[sample_time]
        print(f"  Session: {s.session}")
        print(f"  Regime: {s.regime}")
        print(f"  Active Swings: {len(s.active_swings)}")
        print(f"  Nearest Supp: {s.nearest_support}")
        print(f"  Nearest Res: {s.nearest_resistance}")
