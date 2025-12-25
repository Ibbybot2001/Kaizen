from typing import List
import pandas as pd
import numpy as np
from datetime import timedelta
from schema import StructureEvent, SwingEvent, CompressionEvent, DisplacementEvent, EventType, Direction, ContextTags, Regime, Session

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

    def extract_compressions(self, min_bars: int = 12, max_atr_ratio: float = 0.8) -> List[CompressionEvent]:
        """
        Identifies periods where price is compressing.
        Hypothesis: Volatility contraction leads to expansion.
        
        Logic:
        - Rolling N bars (min_bars)
        - If (Avg True Range of window) < (Series ATR * max_atr_ratio)
        - It is a compression event FROM start_of_window TO current_bar
        
        Optimized Approach:
        - Identify contiguous blocks where condition is met.
        - Merge them into a single 'CompressionEvent'.
        """
        compressions = []
        from schema import CompressionEvent
        
        df = self.df.copy()
        
        # Calculate bar range (High - Low)
        df['tr'] = df['high'] - df['low']
        
        # Calculate 'local volatility' over 'min_bars'
        # We want to see if recent N bars are 'quiet'
        # Let's use the average TR of the last N bars
        df['local_atr'] = df['tr'].rolling(window=min_bars).mean()
        
        # Comparator: Is local_atr < (Global/Session Factor) * Rolling ATR(14)?
        # df['atr'] is already ATR(14).
        
        # Condition: Is the volatility of the last N bars significantly lower than the medium-term ATR?
        df['is_compressed'] = df['local_atr'] < (df['atr'] * max_atr_ratio)
        
        # Filter for True
        compressed_mask = df['is_compressed'].fillna(False)
        
        # Group contiguous True regions
        # Create a group ID that changes every time the condition flips
        df['group_id'] = (compressed_mask != compressed_mask.shift()).cumsum()
        
        # Filter only the groups that are True
        active_groups = df[compressed_mask].groupby('group_id')
        
        for group_id, group_df in active_groups:
            # group_df is a contiguous block of bars that satisfy the compression criteria
            # But wait, 'is_compressed' is a trailing indicator.
            # If at index 100, is_compressed is True, it means bars 88-100 were quiet.
            # We should probably define the event as "Active Compression" starting from when it FIRST became true
            # until it stops being true.
            
            first_bar_idx = group_df.index[0]
            last_bar_idx = group_df.index[-1]
            
            # The actual start of the quiet period was (first_bar_idx - min_bars + 1)
            # But let's just use the detection time for simplicity in Event generation, 
            # or map it back. 
            # Let's say the Event starts when Detection Starts.
            
            start_time = group_df['time'].iloc[0]
            end_time = group_df['time'].iloc[-1]
            
            # Metrics
            avg_tr_during_event = group_df['tr'].mean()
            mean_atr_during_event = group_df['atr'].mean()
            
            # Simple context
            row = group_df.iloc[-1] # Context at end of event
            
            event_id = f"COMP-{start_time.isoformat()}"
            
            context = ContextTags(
                session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER,
                regime=Regime.LOW_VOL, 
                time_of_day=row['time'].strftime('%H:%M'),
                day_of_week=row['time'].dayofweek,
                distance_to_vwap_std=(row['close'] - row['vwap']) / row['atr'] if row['atr'] > 0 else 0
            )

            # Bar count is: (last_bar - first_bar) + min_bars? 
            # Since each True means "The LAST min_bars were compressed", 
            # A sequence of 1 True means a duration of min_bars.
            # A sequence of 2 Trues means min_bars + 1.
            duration_bars = len(group_df) + min_bars - 1
            
            comp_event = CompressionEvent(
                id=event_id,
                event_type=EventType.COMPRESSION,
                start_bar=start_time, # Technically the signal start
                end_bar=end_time,
                direction=Direction.NEUTRAL, # Compression is potential energy, directionless until resolved
                confidence_score=min(1.0, duration_bars / 20.0), # Longer compression = higher confidence?
                context=context,
                bar_count=duration_bars,
                average_true_range=float(avg_tr_during_event),
                compression_ratio=float(avg_tr_during_event / mean_atr_during_event) if mean_atr_during_event > 0 else 0
            )
            
            compressions.append(comp_event)
            
        return compressions

    def extract_displacements(self, min_atr_magnitude: float = 2.0, min_volume_z: float = 1.5) -> List[DisplacementEvent]:
        """
        Identifies Displacement Candles.
        Rules:
        - Range (High-Low) >= min_atr_magnitude * ATR(14)
        - Volume >= Mean_Volume + min_volume_z * Std_Volume (local z-score)
        """
        displacements = []
        from schema import DisplacementEvent # Ensure runtime availability if top-level fails (it shouldn't)
        
        df = self.df.copy()
        
        # 1. ATR Magnitude
        # df['atr'] exists from loader
        df['tr'] = df['high'] - df['low']
        df['atr_mag'] = df['tr'] / df['atr']
        
        # 2. Volume Z-Score
        # Calculate local volume stats (e.g. rolling 20)
        roll_vol_mean = df['volume'].rolling(20).mean()
        roll_vol_std = df['volume'].rolling(20).std()
        df['vol_z'] = (df['volume'] - roll_vol_mean) / roll_vol_std
        
        # Filter
        mask = (df['atr_mag'] >= min_atr_magnitude) & (df['vol_z'] >= min_volume_z)
        
        candidates = df[mask]
        
        for idx, row in candidates.iterrows():
            event_id = f"DISP-{row['time'].isoformat()}"
            
            # Direction? Close > Open = Bullish
            direction = Direction.BULLISH if row['close'] > row['open'] else Direction.BEARISH
            
            context = ContextTags(
                session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER,
                regime=Regime.EXPANSION, 
                time_of_day=row['time'].strftime('%H:%M'),
                day_of_week=row['time'].dayofweek,
                distance_to_vwap_std=(row['close'] - row['vwap']) / row['atr'] if row['atr'] > 0 else 0
            )
            
            # Confidence? Higher Magnitude = Higher Confidence?
            conf = min(1.0, row['atr_mag'] / 5.0) # Cap at 5R?
            
            disp = DisplacementEvent(
                id=event_id,
                event_type=EventType.DISPLACEMENT,
                start_bar=row['time'],
                end_bar=row['time'],
                direction=direction,
                confidence_score=conf,
                context=context,
                magnitude_atr=float(row['atr_mag']),
                volume_z_score=float(row['vol_z']),
                closing_price=float(row['close'])
            )
            displacements.append(disp)
            
        return displacements

if __name__ == "__main__":
    from data_loader import DataLoader
    
    loader = DataLoader(r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv")
    df = loader.load_and_process()
    
    # Run on a subset for speed in test
    subset = df.head(5000)
    
    extractor = StructureExtractor(subset)
    swings = extractor.extract_swings(left_bars=5, right_bars=5)
    
    print(f"Found {len(swings)} swings in 5000 bars")
    
    compressions = extractor.extract_compressions(min_bars=12, max_atr_ratio=0.8)
    print(f"Found {len(compressions)} compression events")
    
    displacements = extractor.extract_displacements(min_atr_magnitude=2.0, min_volume_z=1.5)
    print(f"Found {len(displacements)} displacement events")
    for d in displacements[:3]:
        print(d)
