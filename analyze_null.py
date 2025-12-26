import pandas as pd

def analyze_null_breakdown():
    # Load Logs
    strat = pd.read_csv("logs/run_strategy.csv")
    null = pd.read_csv("logs/run_null.csv")
    
    print("\n--- REGIME BREAKDOWN ---")
    regimes = strat['regime'].unique()
    
    for r in regimes:
        s_r = strat[strat['regime'] == r]['pnl_r']
        n_r = null[null['regime'] == r]['pnl_r']
        
        print(f"Regime: {r}")
        print(f"  Strategy: {s_r.mean():.4f} R ({len(s_r)} trades)")
        print(f"  Null    : {n_r.mean():.4f} R ({len(n_r)} trades)")
        print(f"  Edge    : {s_r.mean() - n_r.mean():.4f} R")
        
    print("\n--- SESSION BREAKDOWN ---")
    sessions = strat['session'].unique()
    
    for s in sessions:
        s_s = strat[strat['session'] == s]['pnl_r']
        n_s = null[null['session'] == s]['pnl_r']
        
        print(f"Session: {s}")
        print(f"  Strategy: {s_s.mean():.4f} R")
        print(f"  Null    : {n_s.mean():.4f} R")
        print(f"  Edge    : {s_s.mean() - n_s.mean():.4f} R")

if __name__ == "__main__":
    analyze_null_breakdown()
