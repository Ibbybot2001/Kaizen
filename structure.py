from typing import List
import pandas as pd
import numpy as np
from datetime import timedelta
from schema import StructureEvent, SwingEvent, CompressionEvent, DisplacementEvent, LiquiditySweepEvent, EventType, Direction, ContextTags, Regime, Session

class StructureExtractor:
    """
    Extracts deterministic market structure events from normalized data.
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
        # Ensure strict datetime sorting
        self.df = self.df.sort_values('time').reset_index(drop=True)

    def extract_swings(self, left_bars: int = 5, right_bars: int = 5, major_factor: int = 3) -> List[SwingEvent]:
        """
        Identifies Standard Pivot Highs and Lows.
        Major Check: Is it also a pivot for [left*factor, right*factor]?
        """
        swings = []
        
        window_size = left_bars + right_bars + 1
        major_window = (left_bars * major_factor) + (right_bars * major_factor) + 1
        
        df = self.df.copy()
        
        # Minor Pivots
        df['max_local'] = df['high'].rolling(window=window_size, center=True).max()
        df['min_local'] = df['low'].rolling(window=window_size, center=True).min()
        
        # Major Pivots (Wider look)
        df['max_major'] = df['high'].rolling(window=major_window, center=True).max()
        df['min_major'] = df['low'].rolling(window=major_window, center=True).min()
        
        pivot_highs = df[df['high'] == df['max_local']]
        pivot_lows = df[df['low'] == df['min_local']]
        
        for idx, row in pivot_highs.iterrows():
            loc_idx = df.index.get_loc(idx)
            conf_idx = loc_idx + right_bars
            
            if conf_idx >= len(df): continue
                
            conf_time = df.iloc[conf_idx]['time']
            
            # Major Check
            # Ensure it is the max in the wider window
            # Note: A Minor swing might NOT be the max of the wider window at the exact same center?
            # Yes, if there is a higher high 10 bars away.
            is_major = (row['high'] == row['max_major'])
            
            event_id = f"SW-H-{row['time'].isoformat()}"
            context = ContextTags(
                session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER, 
                regime=Regime.CHOP,
                time_of_day=row['time'].strftime('%H:%M'),
                day_of_week=row['time'].dayofweek,
                distance_to_vwap_std=0
            )
            
            swing = SwingEvent(
                id=event_id,
                event_type=EventType.SWING_HIGH,
                start_bar=row['time'],
                end_bar=row['time'],
                confirmed_at=conf_time,
                direction=Direction.BEARISH, 
                confidence_score=1.0, 
                context=context,
                price_level=row['high'],
                is_major=is_major # Calculated
            )
            swings.append(swing)

        for idx, row in pivot_lows.iterrows():
            loc_idx = df.index.get_loc(idx)
            conf_idx = loc_idx + right_bars
            
            if conf_idx >= len(df): continue

            conf_time = df.iloc[conf_idx]['time']
            is_major = (row['low'] == row['min_major'])

            event_id = f"SW-L-{row['time'].isoformat()}"
            context = ContextTags(
                session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER,
                regime=Regime.CHOP,
                time_of_day=row['time'].strftime('%H:%M'),
                day_of_week=row['time'].dayofweek,
                distance_to_vwap_std=0
            )
            
            swing = SwingEvent(
                id=event_id,
                event_type=EventType.SWING_LOW,
                start_bar=row['time'],
                end_bar=row['time'],
                confirmed_at=conf_time, 
                direction=Direction.BULLISH, 
                confidence_score=1.0,
                context=context,
                price_level=row['low'],
                is_major=is_major # Calculated
            )
            swings.append(swing)

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
                confirmed_at=end_time, # Confirmed at the close of the block
                direction=Direction.NEUTRAL,
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
                confirmed_at=row['time'], # Confirmed at Bar Close
                direction=direction,
                confidence_score=conf,
                context=context,
                magnitude_atr=float(row['atr_mag']),
                volume_z_score=float(row['vol_z']),
                closing_price=float(row['close'])
            )
            displacements.append(disp)
            
        return displacements

    def extract_sweeps(self, swings: List[SwingEvent], min_reclaim_pts: float = 0.0) -> List[LiquiditySweepEvent]:
        """
        Identifies Liquidity Sweeps (Turtle Soups / Raids).
        Logic:
           Bearish: High > Old_High AND Close < Old_High - min_reclaim_pts
           Bullish: Low < Old_Low AND Close > Old_Low + min_reclaim_pts
        
        CRITICAL: We can only sweep a swing that is CONFIRMED *before* the current bar.
        """
        sweeps = []
        from schema import LiquiditySweepEvent
        
        # Sort swings by confirmation time to ensure availability
        # Actually, for the scanner, we need to know "what swings exist at time T?"
        # We can iterate through the dataframe and maintain an 'active_swings' list 
        # that updates based on confirmed_at.
        
        # Optimization: fast lookup
        # But this is "extraction", usually done batch.
        # Let's do a simple iteration over DF.
        
        df = self.df.sort_values('time').reset_index(drop=True)
        
        # Organize swings by CONFIRMATION time
        swings_by_conf = {}
        for s in swings:
            if s.confirmed_at not in swings_by_conf:
                swings_by_conf[s.confirmed_at] = []
            swings_by_conf[s.confirmed_at].append(s)
            
        active_highs: List[SwingEvent] = []
        active_lows: List[SwingEvent] = []
        
        # To avoid re-sweeping the same level continuously if price chops around it,
        # usually a sweep is a discrete event. Ideally we invalidate the swing after it's swept?
        # Or just log every sweep? The User's strategy implies "Zone Active" upon sweep.
        # Let's log every discrete sweep event.
        
        for idx, row in df.iterrows():
            curr_time = row['time']
            
            # 1. Update Active Structure (born at confirmed_at)
            # Check if any swings are confirmed exactly at this bar? 
            # Or were confirmed since last bar? 
            # Assuming 1m data is continuous-ish.
            
            if curr_time in swings_by_conf:
                new_swings = swings_by_conf[curr_time]
                for s in new_swings:
                    if s.event_type == EventType.SWING_HIGH:
                        active_highs.append(s)
                    elif s.event_type == EventType.SWING_LOW:
                        active_lows.append(s)
            
            # 2. Check for Sweeps against Active Structure
            # We check the MOST RECENT Major/Minor swings usually.
            # User strategy: "lastMajHigh", "lastMinHigh".
            # So we only care about the latest one.
            
            # Bearish Sweep (Sweep High)
            if active_highs:
                last_high = active_highs[-1] # The most recent confirmed high
                
                # Condition: High breached, but Close rejected
                if row['high'] > last_high.price_level and row['close'] < (last_high.price_level - min_reclaim_pts):
                    # Validate: Did this bar JUST breach it? Or was it already above?
                    # "Sweep" implies a raid. 
                    # User strategy logic: "high > lastMajHigh and close < lastMajHigh"
                    # It runs on every bar.
                    
                    event_id = f"SWEEP-H-{curr_time.isoformat()}"
                    
                    context = ContextTags(
                        session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER,
                        regime=Regime.CHOP, # Placeholder
                        time_of_day=row['time'].strftime('%H:%M'),
                        day_of_week=row['time'].dayofweek,
                        distance_to_vwap_std=0.0
                    )
                    
                    sweep = LiquiditySweepEvent(
                        id=event_id,
                        event_type=EventType.LIQUIDITY_SWEEP,
                        start_bar=curr_time,
                        end_bar=curr_time,
                        confirmed_at=curr_time, # Confirmed at Close
                        direction=Direction.BEARISH,
                        confidence_score=1.0,
                        context=context,
                        swept_level=last_high.price_level,
                        sweep_depth=row['high'] - last_high.price_level,
                        swing_id=last_high.id,
                        is_major=last_high.is_major # STRICTLY REQUIRED
                    )
                    sweeps.append(sweep)

            # Bullish Sweep (Sweep Low)
            if active_lows:
                last_low = active_lows[-1]
                
                if row['low'] < last_low.price_level and row['close'] > (last_low.price_level + min_reclaim_pts):
                    event_id = f"SWEEP-L-{curr_time.isoformat()}"
                    
                    context = ContextTags(
                        session=Session(row['session']) if row['session'] in ["ASIA", "LONDON"] else Session.OTHER,
                        regime=Regime.CHOP,
                        time_of_day=row['time'].strftime('%H:%M'),
                        day_of_week=row['time'].dayofweek,
                        distance_to_vwap_std=0.0
                    )
                    
                    sweep = LiquiditySweepEvent(
                        id=event_id,
                        event_type=EventType.LIQUIDITY_SWEEP,
                        start_bar=curr_time,
                        end_bar=curr_time,
                        confirmed_at=curr_time,
                        direction=Direction.BULLISH,
                        confidence_score=1.0,
                        context=context,
                        swept_level=last_low.price_level,
                        sweep_depth=last_low.price_level - row['low'],
                        swing_id=last_low.id,
                        is_major=last_low.is_major
                    )
                    sweeps.append(sweep)
                    
        return sweeps

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
