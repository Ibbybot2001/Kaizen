
import numpy as np
import collections
from datetime import datetime, timedelta

class PineTwin_KaizenV2_Logic:
    """
    Logic Core of 'Kaizen Reversal v2'.
    Strict inputs: OHLCV of Bar N.
    Strict outputs: Signal for Bar N+1 execution.
    NO Execution Logic (Fills/PnL) here.
    """
    

    def __init__(self, use_regime_gate=False, use_cooldown=False, use_usage_gate=False):
        # Configuration Flags (Mode Controls)
        self.use_regime_gate = use_regime_gate
        self.use_cooldown = use_cooldown
        self.use_usage_gate = use_usage_gate

        # Parameters
        self.filters_on = True
        self.cooldown_min = 15
        self.expansion_bars = 10
        self.mintick = 0.25
        
        # State
        self.last_loss_time = 0 
        self.expansion_end_idx = 0
        self.bar_index = -1 
        
        # Structures
        self.prov_maj_high = np.nan
        self.prov_maj_high_idx = np.nan
        self.conf_maj_high = np.nan
        

        self.prov_maj_low = np.nan
        self.prov_maj_low_idx = np.nan
        self.conf_maj_low = np.nan
        
        # Additional State for Strict Mode
        self.conf_maj_high_idx = np.nan
        self.conf_maj_low_idx = np.nan
        
        # Buffers (Parallel lists for speed)
        # Size 30 is enough for High[5] + 5 bars lookback
        self.buf_size = 30
        self.highs = collections.deque(maxlen=self.buf_size)
        self.lows = collections.deque(maxlen=self.buf_size)
        self.closes = collections.deque(maxlen=self.buf_size)
        self.volumes = collections.deque(maxlen=self.buf_size)
        self.times = collections.deque(maxlen=self.buf_size)
        self.indices = collections.deque(maxlen=self.buf_size)
        
        # ATR State (RMA)
        self.prev_atr = np.nan
        self.prev_close = np.nan
        

        # ATR Buffer for Initialization (First 14 bars)
        self.tr_buffer = []
        
        # Usage Gating State
        self.used_levels = set() # Stores 'idx' of levels that have triggered a trade

    def on_bar_close(self, timestamp_ms, open, high, low, close, volume):
        """
        Called when Bar N Creates a Close.
        Returns: Dict or None (Signal for Next Open)
        """
        # Session Filter (RTH: 09:30 - 16:15 ET)
        # Timestamp is UTC ms.
        # Convert to ET.
        dt_utc = datetime.fromtimestamp(timestamp_ms / 1000.0)
        # Approximation: UTC-5 (EST) or UTC-4 (EDT). 
        # For simplicity in 'Twin' without timezone lib dependency, use -5 (Standard)
        # User is in Dec (Standard).
        dt_et = dt_utc - timedelta(hours=5)
        
        t_min = dt_et.hour * 60 + dt_et.minute
        # 9:30 = 570, 16:15 = 975
        if t_min < 570 or t_min > 975:
            # We still need to update buffers/ATRs to keep state valid!
            # But we do NOT trade.
            pass
            
        self.bar_index += 1
        
        # 1. Update Buffers
        self.highs.append(high)
        self.lows.append(low)
        self.closes.append(close)
        self.volumes.append(volume)
        self.times.append(timestamp_ms)
        self.indices.append(self.bar_index)
        
        # 2. ATR Calculation (Strict RMA)
        # TR Calculation
        if np.isnan(self.prev_close):
            # First bar
            tr = high - low
        else:
            tr1 = high - low
            tr2 = abs(high - self.prev_close)
            tr3 = abs(low - self.prev_close)
            tr = max(tr1, tr2, tr3)
        
        self.prev_close = close
        
        atr_val = np.nan
        
        # Initialization Logic
        if np.isnan(self.prev_atr):
            self.tr_buffer.append(tr)
            if len(self.tr_buffer) == 14:
                # Initialize with SMA of first 14 TRs
                self.prev_atr = sum(self.tr_buffer) / 14
                atr_val = self.prev_atr
            elif len(self.tr_buffer) > 14:
                # Should not happen if logic is correct, but safe fallback
                pass
        else:
            # RMA 14
            # alpha = 1/14
            self.prev_atr = (self.prev_atr * 13 + tr) / 14
            atr_val = self.prev_atr
            
        # Data Window Check (Need 20 for Vol MA, 14 for ATR)
        if len(self.closes) < 20 or np.isnan(atr_val):
            return None

        # 3. Volume SMA(20)
        vol_slice = list(self.volumes)[-20:]
        vol_ma = sum(vol_slice) / len(vol_slice)
        

        # 4. Regime (Expansion)
        atr_floor = (atr_val >= (self.mintick * 10))
        is_expansion_event = False
        if atr_floor and ((high - low) >= (1.5 * atr_val)) and (volume >= vol_ma):
            is_expansion_event = True
            # print(f"DEBUG: Expansion Event at {self.bar_index}")
            
        if is_expansion_event:
            self.expansion_end_idx = self.bar_index + self.expansion_bars
            
        is_gated_by_regime = False
        if self.use_regime_gate:
             is_gated_by_regime = self.filters_on and (self.bar_index < self.expansion_end_idx)
        

        # 5. Cooldown
        cooldown_ms = self.cooldown_min * 60 * 1000
        is_cooldown_active = False
        if self.use_cooldown:
             is_cooldown_active = self.filters_on and (timestamp_ms < (self.last_loss_time + cooldown_ms))
        

        # Session Check (re-calc or reuse variable?)
        # Reuse logic: 9:30-16:15
        in_session = (t_min >= 570 and t_min <= 975)
        
        # The Gates (STRICT MODE: Regime + Cooldown + Session)
        is_taking_trades = (not is_gated_by_regime) and (not is_cooldown_active) and in_session

        
        # 6. Structure (Pivot 5,5)
        # Lookback to index -5 (6th from end)
        cand_idx_rel = -6
        highs_list = list(self.highs)
        lows_list = list(self.lows)
        indices_list = list(self.indices)
        
        cand_high = highs_list[cand_idx_rel]
        cand_low = lows_list[cand_idx_rel]
        cand_abs_idx = indices_list[cand_idx_rel]
        

        # Window: -11 to -1 (inclusive of neighbor) in slice notation [-11:]
        # Strict Pivot Check (Pine style: must be strictly > neighbors)
        # Neighbors: -11..-7 (Left 5) and -5..-1 (Right 5)
        # Candidate is at -6
        
        is_ph = True
        is_pl = True
        
        # We need to iterate 10 neighbors
        # Indices relative to buffer end: -11 to -1. Skip -6.
        
        cand_idx = -6 
        
        for i in range(-11, 0): # -11 to -1
            if i == cand_idx: continue
            
            # High Check
            if highs_list[i] >= cand_high: # Strict: If neighbor is >= candidate, not a pivot
                is_ph = False
            
            # Low Check
            if lows_list[i] <= cand_low: # Strict: If neighbor is <= candidate, not a pivot
                is_pl = False
                
            if not is_ph and not is_pl: break

        
        # Provisional (Arming)
        if is_ph:
            if np.isnan(self.prov_maj_high):
                self.prov_maj_high = cand_high
                self.prov_maj_high_idx = cand_abs_idx
        if is_pl:
            if np.isnan(self.prov_maj_low):
                self.prov_maj_low = cand_low
                self.prov_maj_low_idx = cand_abs_idx
                
        # Invalidation (Immediate)
        if not np.isnan(self.prov_maj_high):
            if high > self.prov_maj_high:
                self.prov_maj_high = np.nan
                self.prov_maj_high_idx = np.nan
        if not np.isnan(self.prov_maj_low):
            if low < self.prov_maj_low:
                self.prov_maj_low = np.nan
                self.prov_maj_low_idx = np.nan
                

        # Confirmation (Age >= 15)
        # Note: We need to PERSIST the ID (Index) when confirming
        # Since we don't store separate conf_maj_idx, we assume that once confirmed,
        # the provisional index is effectively the ID. But prov_maj clears.
        # FIX: We need a mapping or just trust the Coordinates.
        # Let's simple use: ID = Price (float) * 10000 (int) or similar? 
        # No, duplicate prices possible.
        # Correct approach: Add `self.conf_maj_high_idx` state. 
        # But for now, let's assume `active_maj_high` logic needs to be patched.
        
        # NOTE: I am adding `self.conf_maj_high_idx` dynamically here by just setting it on self
        # Python allows dynamic attributes.
        
        if not np.isnan(self.prov_maj_high):
            if (self.bar_index - self.prov_maj_high_idx) >= 15:
                self.conf_maj_high = self.prov_maj_high
                self.conf_maj_high_idx = self.prov_maj_high_idx # New attr
                self.prov_maj_high = np.nan
                self.prov_maj_high_idx = np.nan
        if not np.isnan(self.prov_maj_low):
            if (self.bar_index - self.prov_maj_low_idx) >= 15:
                self.conf_maj_low = self.prov_maj_low
                self.conf_maj_low_idx = self.prov_maj_low_idx # New attr
                self.prov_maj_low = np.nan
                self.prov_maj_low_idx = np.nan
        
        # Active Levels with IDs
        active_high = np.nan
        active_high_id = -1
        
        if not np.isnan(self.conf_maj_high):
             active_high = self.conf_maj_high
             active_high_id = getattr(self, 'conf_maj_high_idx', -1)
        elif not np.isnan(self.prov_maj_high):
             active_high = self.prov_maj_high
             active_high_id = self.prov_maj_high_idx
             
        active_low = np.nan
        active_low_id = -1
        
        if not np.isnan(self.conf_maj_low):
             active_low = self.conf_maj_low
             active_low_id = getattr(self, 'conf_maj_low_idx', -1)
        elif not np.isnan(self.prov_maj_low):
             active_low = self.prov_maj_low
             active_low_id = self.prov_maj_low_idx
        
        # 7. Triggers
        signal = None
        target_sl = np.nan
        atr_val_final = atr_val
        
        if not np.isnan(active_high) and is_taking_trades:
            # Check Usage
            if active_high_id not in self.used_levels:
                if high > active_high:
                    if close <= (active_high + (atr_val * 0.05)):
                        signal = 'SHORT'
                        target_sl = active_high + (atr_val * 0.25)
                        self.used_levels.add(active_high_id)
                    
        if not np.isnan(active_low) and is_taking_trades and (signal is None):
            if active_low_id not in self.used_levels:
                if low < active_low:
                    if close >= (active_low - (atr_val * 0.05)):
                        signal = 'LONG'
                        target_sl = active_low - (atr_val * 0.25)
                        self.used_levels.add(active_low_id)
                    
        # Return Signal Packet
        if signal:
            return {
                'signal': signal,
                'sl': target_sl,
                'atr': atr_val_final
            }
        return None

    def record_loss(self, timestamp_ms):
        """Called by Runner when a loss actually occurs"""
        self.last_loss_time = timestamp_ms
