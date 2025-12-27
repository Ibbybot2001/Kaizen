import sys
import os
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_loader import DataLoader
from live.pine_twin import PineTwin_KaizenV2_Logic

def run_sweep():
    data_path = r"C:\Users\CEO\.gemini\antigravity\scratch\kaizen_1m_data_ibkr_2yr.csv"
    print(f"Loading data from {data_path}...")
    loader = DataLoader(data_path)
    df = loader.load_and_process()
    print(f"Loaded {len(df)} bars.")

    multipliers = [1.5, 1.4, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8]
    results = []
    
    rr_target = 2.0
    mintick = 0.25
        
    for mult in multipliers:
        print(f"--- Sweeping Expansion Mult: {mult} ---")
        
        # Init Strict Mode with specific mult
        twin = PineTwin_KaizenV2_Logic(
            use_regime_gate=True,
            use_cooldown=True,
            use_usage_gate=True,
            expansion_mult=mult
        )
        
        closed_trades = []
        equity_curve = [100000.0]
        
        # Execution State
        current_pos = 0 
        entry_price = np.nan
        entry_sl = np.nan
        entry_tp = np.nan
        
        pending_signal = None 
        
        count = 0
        
        # --- SIMULATION LOOP (Simplified from run_twin) ---
        for idx, row in df.iterrows():
            ts_ms = int(row['time'].timestamp() * 1000)
            row_open = row['open']
            row_high = row['high']
            row_low = row['low']
            row_close = row['close']
            
            # 1. Execution
            exit_triggered = False
            pnl = 0
            
            if current_pos != 0:
                if current_pos == 1: # Long
                    if row_low <= entry_sl:
                        pnl = (entry_sl - entry_price) * 2 
                        twin.record_loss(ts_ms) 
                        exit_triggered = True
                    elif row_high >= entry_tp:
                        pnl = (entry_tp - entry_price) * 2
                        exit_triggered = True
                elif current_pos == -1: # Short
                    if row_high >= entry_sl:
                        pnl = (entry_price - entry_sl) * 2
                        twin.record_loss(ts_ms)
                        exit_triggered = True
                    elif row_low <= entry_tp:
                        pnl = (entry_price - entry_tp) * 2
                        exit_triggered = True
                        
                if exit_triggered:
                    closed_trades.append({'pnl': pnl})
                    equity_curve.append(equity_curve[-1] + pnl)
                    current_pos = 0
                    entry_price = np.nan

            if pending_signal:
                sig_type = pending_signal['signal']
                sig_sl = pending_signal['sl']
                fill_price = row_open
                risk_dist = abs(fill_price - sig_sl)
                if risk_dist < (mintick * 5): risk_dist = (mintick * 5)
                
                if sig_type == 'LONG':
                    target_tp = fill_price + (risk_dist * rr_target)
                    new_pos = 1
                else:
                    target_tp = fill_price - (risk_dist * rr_target)
                    new_pos = -1
                
                # Reverse Logic
                if current_pos != 0:
                    flip_pnl = 0
                    if current_pos == 1: flip_pnl = (fill_price - entry_price) * 2
                    else: flip_pnl = (entry_price - fill_price) * 2
                    
                    closed_trades.append({'pnl': flip_pnl})
                    equity_curve.append(equity_curve[-1] + flip_pnl)
                    if flip_pnl < 0: twin.record_loss(ts_ms)
                
                current_pos = new_pos
                entry_price = fill_price
                entry_sl = sig_sl
                entry_tp = target_tp
                pending_signal = None
                
                # Same Bar Exit Check
                sb_pnl = 0
                sb_exit = False
                if current_pos == 1:
                     if row_low <= entry_sl:
                        sb_pnl = (entry_sl - entry_price) * 2
                        twin.record_loss(ts_ms)
                        sb_exit = True
                     elif row_high >= entry_tp:
                        sb_pnl = (entry_tp - entry_price) * 2
                        sb_exit = True
                elif current_pos == -1:
                     if row_high >= entry_sl:
                        sb_pnl = (entry_price - entry_sl) * 2
                        twin.record_loss(ts_ms)
                        sb_exit = True
                     elif row_low <= entry_tp:
                        sb_pnl = (entry_price - entry_tp) * 2
                        sb_exit = True
                
                if sb_exit:
                    closed_trades.append({'pnl': sb_pnl})
                    equity_curve.append(equity_curve[-1] + sb_pnl)
                    current_pos = 0

            # 2. Logic
            new_sig = twin.on_bar_close(ts_ms, row_open, row_high, row_low, row_close, row['volume'])
            if new_sig: pending_signal = new_sig
            
            count += 1
            if count % 200000 == 0: print(f"Processed {count}...")
            
        # --- END SIMULATION ---
        
        # Calculate Metrics
        total_trades = len(closed_trades)
        win_rate = 0.0
        avg_pnl = 0.0
        total_pnl = 0.0
        max_dd = 0.0
        
        if total_trades > 0:
            df_res = pd.DataFrame(closed_trades)
            wins = df_res[df_res['pnl'] > 0]
            win_rate = len(wins) / total_trades * 100
            total_pnl = df_res['pnl'].sum()
            avg_pnl = df_res['pnl'].mean()
            
            # Max DD
            eq = np.array(equity_curve)
            peak = np.maximum.accumulate(eq)
            dd = (peak - eq)
            max_dd = np.max(dd)
            
        print(f"Result: {mult} -> {total_trades} trades, {win_rate:.1f}% WR, ${total_pnl:.0f} PnL")
        
        results.append({
            'expansion_mult': mult,
            'trades': total_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'max_dd': max_dd
        })
        
    # Export
    df_results = pd.DataFrame(results)
    out_csv = r"c:\Users\CEO\.gemini\antigravity\scratch\shbe\analysis\expansion_sweep.csv"
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df_results.to_csv(out_csv, index=False)
    
    # Generate MD Report
    markdown = "# Expansion Threshold Parameter Sweep\n\n"
    markdown += "| Multiplier | Trades (2yr) | Trades/Year | Win Rate | Total PnL | Avg PnL | Max DD |\n"
    markdown += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
    
    for r in results:
        trades_yr = r['trades'] / 2.0
        markdown += f"| {r['expansion_mult']} ATR | {r['trades']} | {trades_yr:.1f} | {r['win_rate']:.2f}% | ${r['total_pnl']:.0f} | ${r['avg_pnl']:.2f} | ${r['max_dd']:.0f} |\n"
        
    markdown += "\n## Interpretation\n"
    markdown += "This sweep identifies the threshold where the strategy transitions from 'noisy' to 'selective'.\n"
    markdown += "Target Zone: 40-60 Trades/Year.\n"
    
    with open(r"c:\Users\CEO\.gemini\antigravity\scratch\shbe\analysis\expansion_sweep_report.md", "w") as f:
        f.write(markdown)
        
    print("Sweep Complete. Report and CSV generated.")

if __name__ == "__main__":
    run_sweep()
