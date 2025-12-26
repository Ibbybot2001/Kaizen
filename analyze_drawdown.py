import pandas as pd
import numpy as np

def analyze_dd():
    print("Loading Strategy Log...")
    df = pd.read_csv("logs/run_strategy.csv")
    
    # 1. Equity & Drawdown Curve
    df['cum_r'] = df['pnl_r'].cumsum()
    df['peak_r'] = df['cum_r'].cummax()
    df['dd_r'] = df['cum_r'] - df['peak_r']
    
    # MAX DD Details
    max_dd = df['dd_r'].min()
    max_dd_idx = df['dd_r'].idxmin()
    peak_before_dd_idx = df.loc[:max_dd_idx, 'cum_r'].idxmax()
    
    dd_duration_trades = max_dd_idx - peak_before_dd_idx
    print(f"\n--- MAX DRAWDOWN DECOMPOSITION ---")
    print(f"Max Drawdown: {max_dd:.2f} R")
    print(f"Peak at Trade #{peak_before_dd_idx} -> Low at Trade #{max_dd_idx}")
    print(f"Duration: {dd_duration_trades} Trades")
    
    # 2. Clusters vs Small Losses
    # Analyze streaks of losses in the worst DD period
    dd_period = df.iloc[peak_before_dd_idx:max_dd_idx+1]
    
    # Calculate streaks
    dd_period['is_loss'] = dd_period['pnl_r'] < 0
    # Group consecutive losses
    streak_ids = (dd_period['is_loss'] != dd_period['is_loss'].shift()).cumsum()
    streaks = dd_period[dd_period['is_loss']].groupby(streak_ids).size()
    
    print(f"\n[Inside the Worst DD Period]")
    print(f"Total Trades: {len(dd_period)}")
    print(f"Win Rate: {(len(dd_period[dd_period['result']=='WIN']) / len(dd_period) * 100):.1f}%")
    print(f"Max Consecutive Losses: {streaks.max() if not streaks.empty else 0}")
    print(f"Average Loss Streak: {streaks.mean() if not streaks.empty else 0:.1f}")
    
    # 3. Regime Transitions
    # Did DD accelerate after transition?
    # Identify change points
    df['regime_shift'] = df['regime'] != df['regime'].shift()
    transitions = df[df['regime_shift']].index
    
    # Check average PnL in the 50 trades AFTER a regime change vs Normal
    post_trans_pnl = []
    normal_pnl = []
    
    # Mark 'post transition' zones
    df['is_post_transition'] = False
    for t_idx in transitions:
        # Mark next 50 trades
        range_end = min(t_idx + 50, len(df))
        df.loc[t_idx:range_end, 'is_post_transition'] = True
        
    pnl_post = df[df['is_post_transition']]['pnl_r'].mean()
    pnl_norm = df[~df['is_post_transition']]['pnl_r'].mean()
    
    print(f"\n[Regime Transition Impact]")
    print(f"Avg PnL (Post-Transition 50 trades): {pnl_post:.4f} R")
    print(f"Avg PnL (Stable Regime): {pnl_norm:.4f} R")
    
    # 4. Filter Worst 5% of Periods
    # We define "Periods" as rolling windows of 100 trades?
    # Or simply identifying the trades contributing to the steepest drawdowns.
    # Let's take Rolling 100-trade PnL.
    
    df['rolling_100_pnl'] = df['pnl_r'].rolling(100).sum()
    threshold_5pct = df['rolling_100_pnl'].quantile(0.05)
    
    print(f"\n[Sensitivity Analysis]")
    print(f"Worst 5% Rolling 100-trade PnL Threshold: < {threshold_5pct:.2f} R")
    
    # Identify trades that belong to these "Toxic Windows"
    # If a trade falls inside a window that is in the bottom 5%, mark it?
    # A trade belongs to 100 overlapping windows.
    # Logic: If a trade is part of ANY window < threshold? No, that excludes too much.
    # Logic: If a trade is part of the CENTER of a toxic window?
    
    # Simplified approach: Remove the specific rolling windows?
    # Better: Remove the trades that occurred during the "Drawdown Phases".
    # Calculated earlier: dd_r.
    # Let's define "Worst 5% of Drawdown DEPTH" (the bottom 5% of equity curve values relative to peak?)
    # No, User asks "Worst 5% of drawdown periods".
    # Interpreted: The periods where PnL was worst.
    # Let's use the Rolling PnL definition.
    # Mask trades where the *current* rolling 100 sum is in the bottom 5%.
    
    mask_toxic = df['rolling_100_pnl'] < threshold_5pct
    # Note: This is look-ahead bias if used for filtering LIVE, but valid for "What if" analysis.
    
    clean_df = df[~mask_toxic]
    new_mean = clean_df['pnl_r'].mean()
    new_total = len(clean_df)
    
    print(f"Original Expectancy: {df['pnl_r'].mean():.4f} R")
    print(f"Adjusted Expectancy (removing worst 5% rolling periods): {new_mean:.4f} R")
    print(f"Trades Removed: {len(df) - new_total}")

if __name__ == "__main__":
    analyze_dd()
