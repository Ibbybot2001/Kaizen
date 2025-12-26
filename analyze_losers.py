import pandas as pd
import numpy as np

def analyze_losses():
    try:
        df = pd.read_csv("logs/run_strategy_enriched.csv")
    except Exception as e:
        print(f"Failed to load log: {e}")
        return

    # Parse Time
    df['trigger_time'] = pd.to_datetime(df['trigger_time'])
    df['is_win'] = df['result'] == 'WIN'
    
    print(f"Analysing {len(df)} Trades ({df['is_win'].sum()} Wins, {(~df['is_win']).sum()} Losses)")
    
    # 1. Feature Engineering
    # Time of Day (Hour)
    df['hour'] = df['trigger_time'].dt.hour
    
    # Time Since Last Sweep (Cluster Detection)
    df = df.sort_values('trigger_time')
    df['time_diff_min'] = df['trigger_time'].diff().dt.total_seconds() / 60.0
    # Fill first NaN with large number
    df['time_diff_min'] = df['time_diff_min'].fillna(9999)
    
    # VWAP Dist (Absolute)
    df['vwap_dist_abs'] = df['vwap_dist'].abs()
    
    # 2. Compare Distributions (Win vs Loss)
    features = ['hour', 'vwap_dist', 'sweep_depth', 'time_diff_min']
    
    print("\n[Feature Distribution: Win vs Loss]")
    for feat in features:
        if feat not in df.columns: continue
        
        mu_win = df[df['is_win']][feat].mean()
        mu_loss = df[~df['is_win']][feat].mean()
        
        # Simple Cohen's d or just % diff
        diff_pct = (mu_loss - mu_win) / mu_win * 100 if mu_win != 0 else 0
        
        print(f"{feat.upper()}: Win={mu_win:.2f} | Loss={mu_loss:.2f} | Diff={diff_pct:+.1f}%")
        
        # Specific Insight Checks
        if feat == 'time_diff_min':
            # Are losses clustered? (Lower time diff)
            if mu_loss < mu_win:
                print(f"  -> WARNING: Losses occur in tighter clusters ({mu_loss:.1f}m vs {mu_win:.1f}m)")
        
        if feat == 'vwap_dist':
            # Do we lose when far from VWAP?
            pass

    # 3. Categorical Analysis
    print("\n[Categorical Win Rates]")
    
    # Hour Block
    df['hour_block'] = pd.cut(df['hour'], bins=[0, 3, 9, 10, 12, 16, 24], labels=['Asia', 'Pre-Lon', 'Pre-NY', 'NY-Open', 'NY-Lunch', 'Post-Close'])
    print(df.groupby('hour_block')['is_win'].mean())
    
    # Major vs Minor (if available) - checking 'is_major' column
    if 'is_major' in df.columns:
        print("\n[Major vs Minor Structure]")
        print(df.groupby('is_major')['pnl_r'].agg(['count', 'mean']))

    # 4. "Toxic Feature" Identification
    # Find the decile of each feature with the lowest Win Rate
    print("\n[Toxic Segments]")
    for feat in ['vwap_dist', 'sweep_depth', 'time_diff_min']:
        try:
            df['bin'] = pd.qcut(df[feat], 10, duplicates='drop')
            grouped = df.groupby('bin')['pnl_r'].mean()
            worst_bin = grouped.idxmin()
            worst_val = grouped.min()
            print(f"Worst {feat} range: {worst_bin} -> Expectancy {worst_val:.2f} R")
        except:
            pass

if __name__ == "__main__":
    analyze_losses()
