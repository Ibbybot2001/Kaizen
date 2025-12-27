import pandas as pd
import numpy as np
import os

def analyze():
    # Load Data
    try:
        df_sampling = pd.read_csv(r"c:\Users\CEO\.gemini\antigravity\scratch\shbe\live\trades_sampling_mode.csv")
        df_strict = pd.read_csv(r"c:\Users\CEO\.gemini\antigravity\scratch\shbe\live\trades_strict_mode.csv")
    except FileNotFoundError:
        print("Error: Could not find trade CSVs.")
        return

    print("Data Loaded.")
    print(f"Sampling: {len(df_sampling)} | Strict: {len(df_strict)}")

    # 1. Identify Survivors
    # Key: Exit Time + Active Level (Since multiple trades can happen at same time if reversed?)
    # Simplest unique key: exit_time (ms). 
    # Strict trades are a subset.
    
    strict_ids = set(df_strict['exit_time'].astype(str) + "_" + df_strict['active_level'].astype(str))
    
    # Label Sampling trades
    df_sampling['trade_id'] = df_sampling['exit_time'].astype(str) + "_" + df_sampling['active_level'].astype(str)
    df_sampling['status'] = df_sampling['trade_id'].apply(lambda x: 'SURVIVOR' if x in strict_ids else 'DEAD')
    df_sampling['outcome'] = df_sampling['pnl'].apply(lambda x: 'WIN' if x > 0 else 'LOSS')
    
    # 2. Export survivors_vs_dead.csv
    export_cols = [
        'mode', 'status', 'outcome', 'pnl', 'reason', 
        'structure_age', 'reclaim_depth', 'is_expansion', 'retest_count', 'active_level', 'exit_time'
    ]
    # Check if all columns exist (some might be missing in header if NaN?)
    # The header checked previously showed them.
    
    out_csv = r"c:\Users\CEO\.gemini\antigravity\scratch\shbe\analysis\survivors_vs_dead.csv"
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df_sampling[export_cols].to_csv(out_csv, index=False)
    print(f"Generated: {out_csv}")
    
    # 3. Generate Reports
    generate_stratification_report(df_sampling)
    generate_filters_report(df_sampling)

def generate_stratification_report(df):
    survivors = df[df['status'] == 'SURVIVOR']
    dead = df[df['status'] == 'DEAD']
    
    surv_wr = (len(survivors[survivors['pnl']>0]) / len(survivors) * 100) if len(survivors) > 0 else 0
    dead_wr = (len(dead[dead['pnl']>0]) / len(dead) * 100) if len(dead) > 0 else 0
    
    report = f"""# Stratification Analysis Report

## High-Level Findings
*   **Total Trades (Sampling):** {len(df)}
*   **Survivors (Strict):** {len(survivors)} ({len(survivors)/len(df)*100:.1f}%)
*   **Casualties (Dead):** {len(dead)} ({len(dead)/len(df)*100:.1f}%)

## Performance Delta
| Cohort | Count | Win Rate | Total PnL |
|:--- |:--- |:--- |:--- |
| **Survivors** | {len(survivors)} | **{surv_wr:.2f}%** | ${survivors['pnl'].sum():.2f} |
| **Dead** | {len(dead)} | {dead_wr:.2f}% | ${dead['pnl'].sum():.2f} |

## Structural Insights

### 1. Structure Age (Freshness)
*   **Avg Age (Survivors):** {survivors['structure_age'].mean():.1f} bars
*   **Avg Age (Dead):** {dead['structure_age'].mean():.1f} bars
*(Is older structure better or worse?)*

### 2. Retest Count (Usage)
*   **Avg Retest (Survivors):** {survivors['retest_count'].mean():.1f}
*   **Avg Retest (Dead):** {dead['retest_count'].mean():.1f}
*(Survivors should be mostly 1st/2nd retests due to usage gating)*

### 3. Expansion
*   **% Expansion Trades (Survivors):** {len(survivors[survivors['is_expansion']==True])/len(survivors)*100:.1f}%
*   **% Expansion Trades (Dead):** {len(dead[dead['is_expansion']==True])/len(dead)*100:.1f}%
"""
    with open(r"c:\Users\CEO\.gemini\antigravity\scratch\shbe\analysis\stratification_report.md", "w", encoding='utf-8') as f:
        f.write(report)
    print("Generated: stratification_report.md")

def generate_filters_report(df):
    # This implies we know WHICH filter killed it.
    # We don't have that granular flag (yet). 
    # But we can infer properties.
    
    # Usage Gate Effectiveness:
    # Dead trades with retest_count > 1?
    usage_dead = df[(df['status']=='DEAD') & (df['retest_count'] > 1)]
    usage_dead_wr = len(usage_dead[usage_dead['pnl']>0])/len(usage_dead)*100 if len(usage_dead) > 0 else 0
    
    # Expansion Gate Effectiveness:
    # Dead trades inside expansion?
    exp_dead = df[(df['status']=='DEAD') & (df['is_expansion'] == True)]
    exp_dead_wr = len(exp_dead[exp_dead['pnl']>0])/len(exp_dead)*100 if len(exp_dead) > 0 else 0

    report = f"""# Filters Effectiveness

## 1. Usage Gate (Retest Count > 1)
*   **Removed Count:** {len(usage_dead)}
*   **Removed Win Rate:** {usage_dead_wr:.2f}%
*   **Verdict:** Removing these trades {'IMPROVED' if usage_dead_wr < 37.9 else 'HURT'} the baseline.

## 2. Expansion Gate (Trades during Expansion)
*   **Removed Count:** {len(exp_dead)}
*   **Removed Win Rate:** {exp_dead_wr:.2f}%
*   **Verdict:** Removing these trades {'IMPROVED' if exp_dead_wr < 37.9 else 'HURT'} the baseline.
"""
    with open(r"c:\Users\CEO\.gemini\antigravity\scratch\shbe\analysis\filters_effectiveness.md", "w", encoding='utf-8') as f:
        f.write(report)
    print("Generated: filters_effectiveness.md")

if __name__ == "__main__":
    analyze()
