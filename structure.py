from typing import List
import pandas as pd
import numpy as np
from datetime import timedelta
from schema import StructureEvent, SwingEvent, EventType, Direction, ContextTags, Regime, Session

class StructureExtractor:
    """
    Extracts deterministic market structure events from normalized data.
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
        # Ensure strict datetime sorting
        self.df = self.df.sort_values('time').reset_index(drop=True)

    def extract_swings(self, left_bars: int = 5, right_bars: int = 5) -> List[SwingEvent]:
        """
        Identifies Standard Pivot Highs and Lows.
        Rules:
        - High > Highs of [left_bars]
        - High > Highs of [right_bars]
        - Similar logic for Lows
        """
        swings = []
        
        # Note: We need 'right_bars' of future data to confirm a swing.
        # Ideally, we iterate through the dataframe. For performance, we can vectorize.
        # But to create rich 'SwingEvent' objects, iteration or hybrid approach is best.
        
        # Using rolling windows for efficiency
        # Shifted windows to center the point of interest
        
        # Determine High Pivots
        # roll_max(window=left+right+1, center=True) checks the surroundings
        # If current high == roll_max, it's a potential pivot.
        
        window_size = left_bars + right_bars + 1
        
        # We must use 'shift' carefully. 
        # rolling(center=True) looks ahead. In a backtest engine, we can compute this upfront for history.
        # But 'confidence_score' implies we might not establish it immediately at bar 0?
        # Actually, for a pure backtest on historical data, we can just extract them all.
        
        df = self.df.copy()
        
        df['max_local'] = df['high'].rolling(window=window_size, center=True).max()
        df['min_local'] = df['low'].rolling(window=window_size, center=True).min()
        
        # Identify Pivot High candidates
        # A bar is max if high == max_local AND it's not NaN
        pivot_highs = df[df['high'] == df['max_local']]
        
        # Identify Pivot Low candidates
        pivot_lows = df[df['low'] == df['min_local']]
        
        # Process Highs
        for idx, row in pivot_highs.iterrows():
            # Verify strict left/right isolation (rolling includes the bar itself, usually fine)
            # Need to be careful of equal highs. For now, accept the first or all.
            # Let's ensure strict strict definition checking if needed. 
            # Rolling max logic is robust for standard definition.
            
            # Construct Event
            # Context extraction
            # We need simple helpers for ContextTags. 
            # For now, hardcode/simplify to prove the point.
            
            event_id = f"SW-H-{row['time'].isoformat()}"
            
            # Note: The 'End Bar' of a Swing Event is technically when it is CONFIRMED (i.e. right_bars later)
            # But the swing itself happened at 'time'.
            # Let's set start_bar = swing time, end_bar = swing time (instantaneous event?)
            # Or end_bar = confirmation time? 
            # Let's stick to start/end = swing bar for the EVENT itself, 
            # but the 'extraction' system knows it appeared later.
            
            context = ContextTags(
                session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER, 
                # ^ Simple map, need strict mapping later
                regime=Regime.CHOP, # Placeholder algo
                time_of_day=row['time'].strftime('%H:%M'),
                day_of_week=row['time'].dayofweek,
                distance_to_vwap_std=(row['high'] - row['vwap']) / row['atr'] if row['atr'] > 0 else 0
            )
            
            swing = SwingEvent(
                id=event_id,
                event_type=EventType.SWING_HIGH,
                start_bar=row['time'],
                end_bar=row['time'],
                direction=Direction.BEARISH, # Highs mark resistance (bearish reaction point)
                confidence_score=1.0, # It is mathematically a pivot
                context=context,
                price_level=row['high'],
                is_major=False # Needs higher timeframe logic
            )
            swings.append(swing)

        # Process Lows
        for idx, row in pivot_lows.iterrows():
            event_id = f"SW-L-{row['time'].isoformat()}"
            
            context = ContextTags(
                session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER,
                regime=Regime.CHOP,
                time_of_day=row['time'].strftime('%H:%M'),
                day_of_week=row['time'].dayofweek,
                distance_to_vwap_std=(row['low'] - row['vwap']) / row['atr'] if row['atr'] > 0 else 0
            )
            
            swing = SwingEvent(
                id=event_id,
                event_type=EventType.SWING_LOW,
                start_bar=row['time'],
                end_bar=row['time'],
                direction=Direction.BULLISH, # Lows mark support (bullish reaction point)
                confidence_score=1.0,
                context=context,
                price_level=row['low'],
                is_major=False
            )
            swings.append(swing)

        # Sort by time
        swings.sort(key=lambda x: x.start_bar)
        
        return swings

if __name__ == "__main__":
    from data_loader import DataLoader
    
    loader = DataLoader(r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv")
    df = loader.load_and_process()
    
    # Run on a subset for speed in test
    subset = df.head(5000)
    
    extractor = StructureExtractor(subset)
    swings = extractor.extract_swings(left_bars=5, right_bars=5)
    
    print(f"Found {len(swings)} swings in 5000 bars")
    for s in swings[:3]:
        print(s)
