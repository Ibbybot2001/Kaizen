
import sys
import os
import pandas as pd
import numpy as np
import math

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_loader import DataLoader
from live.pine_twin import PineTwin_KaizenV2_Logic


def run_simulation(df, mode_name, use_regime, use_cooldown, use_usage):
    print(f"--- Running Simulation: {mode_name} ---")
    twin = PineTwin_KaizenV2_Logic(
        use_regime_gate=use_regime, 
        use_cooldown=use_cooldown, 
        use_usage_gate=use_usage
    )
    
    closed_trades = []
    
    # Execution State
    current_pos = 0 # 1 or -1
    entry_price = np.nan
    entry_sl = np.nan
    entry_tp = np.nan
    
    # Limit Order Queue (Signal at Close -> Execute at Next Open)
    pending_signal = None 
    
    rr_target = 2.0
    mintick = 0.25
    
    count = 0
    for idx, row in df.iterrows():
        ts_ms = int(row['time'].timestamp() * 1000)
        row_open = row['open']
        row_high = row['high']
        row_low = row['low']
        row_close = row['close']
        
        # 1. EXECUTION PHASE (At Open of this Bar)
        exit_triggered = False
        
        if current_pos != 0:
            pnl = 0
            reason = ""
            
            if current_pos == 1: # Long
                if row_low <= entry_sl:
                    pnl = (entry_sl - entry_price) * 2 # $2/pt
                    reason = "SL Hit"
                    exit_triggered = True
                    twin.record_loss(ts_ms) 
                elif row_high >= entry_tp:
                    pnl = (entry_tp - entry_price) * 2
                    reason = "TP Hit"
                    exit_triggered = True
            elif current_pos == -1: # Short
                if row_high >= entry_sl:
                    pnl = (entry_price - entry_sl) * 2
                    reason = "SL Hit"
                    exit_triggered = True
                    twin.record_loss(ts_ms)
                elif row_low <= entry_tp:
                    pnl = (entry_price - entry_tp) * 2
                    reason = "TP Hit"
                    exit_triggered = True
                    
            if exit_triggered:
                closed_trades.append({
                    'exit_time': ts_ms,
                    'pnl': pnl,
                    'reason': reason
                })
                current_pos = 0
                entry_price = np.nan
        
        # B. Execute PENDING Entries (from Prev Close)
        if pending_signal:
            sig_type = pending_signal['signal']
            sig_sl = pending_signal['sl']
            sig_atr = pending_signal['atr']
            
            fill_price = row_open
            
            risk_dist = abs(fill_price - sig_sl)
            if risk_dist < (mintick * 5): risk_dist = (mintick * 5)
            
            if sig_type == 'LONG':
                target_tp = fill_price + (risk_dist * rr_target)
                new_pos = 1
            else:
                target_tp = fill_price - (risk_dist * rr_target)
                new_pos = -1
            
            # Reversal Handling
            if current_pos != 0:
                flip_pnl = 0
                if current_pos == 1:
                    flip_pnl = (fill_price - entry_price) * 2
                else:
                    flip_pnl = (entry_price - fill_price) * 2
                
                closed_trades.append({
                    'exit_time': ts_ms,
                    'pnl': flip_pnl,
                    'reason': "Reverse"
                })
                if flip_pnl < 0:
                    twin.record_loss(ts_ms)
            
            current_pos = new_pos
            entry_price = fill_price
            entry_sl = sig_sl
            entry_tp = target_tp
            
            pending_signal = None
            
            # Valid Entry Clean Check (Same Bar Exit)
            if current_pos == 1:
                 if row_low <= entry_sl:
                    pnl = (entry_sl - entry_price) * 2
                    reason = "SL Hit (Same Bar)"
                    twin.record_loss(ts_ms)
                    closed_trades.append({'exit_time': ts_ms, 'pnl': pnl, 'reason': reason})
                    current_pos = 0
                 elif row_high >= entry_tp:
                    pnl = (entry_tp - entry_price) * 2
                    reason = "TP Hit (Same Bar)"
                    closed_trades.append({'exit_time': ts_ms, 'pnl': pnl, 'reason': reason})
                    current_pos = 0
            elif current_pos == -1:
                 if row_high >= entry_sl:
                    pnl = (entry_price - entry_sl) * 2
                    reason = "SL Hit (Same Bar)"
                    twin.record_loss(ts_ms)
                    closed_trades.append({'exit_time': ts_ms, 'pnl': pnl, 'reason': reason})
                    current_pos = 0
                 elif row_low <= entry_tp:
                    pnl = (entry_price - entry_tp) * 2
                    reason = "TP Hit (Same Bar)"
                    closed_trades.append({'exit_time': ts_ms, 'pnl': pnl, 'reason': reason})
                    current_pos = 0

        # 2. LOGIC PHASE (At Close of this Bar)
        new_sig = twin.on_bar_close(ts_ms, row_open, row_high, row_low, row_close, row['volume'])
        
        if new_sig:
            pending_signal = new_sig
            
        count += 1
        if count % 100000 == 0:
            print(f"Processed {count}...")

    # Report
    print(f"[{mode_name}] Total Trades: {len(closed_trades)}")
    if closed_trades:
        df_res = pd.DataFrame(closed_trades)
        wins = df_res[df_res['pnl'] > 0]
        print(f"[{mode_name}] Win Rate: {len(wins)/len(closed_trades)*100:.2f}%")
        print(f"[{mode_name}] Total PnL: {df_res['pnl'].sum():.2f}")
        return len(closed_trades), df_res['pnl'].sum()
    return 0, 0.0

def run_backtest():
    data_path = r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv"
    print(f"Loading data from {data_path}...")
    loader = DataLoader(data_path)
    df = loader.load_and_process()
    print(f"Loaded {len(df)} bars.")

    # 1. SAMPLING MODE (All Gates OFF)
    run_simulation(df, "SAMPLING_MODE", use_regime=False, use_cooldown=False, use_usage=False)

    # 2. STRICT MODE (All Gates ON)
    run_simulation(df, "STRICT_MODE", use_regime=True, use_cooldown=True, use_usage=True)

if __name__ == "__main__":
    run_backtest()
