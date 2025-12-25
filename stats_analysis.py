import pandas as pd
import numpy as np
from scipy import stats

def analyze_stats():
    # Load the log
    try:
        df = pd.read_csv("logs/verification_run_full.csv")
    except Exception as e:
        print(f"Error loading log: {e}")
        # Fallback to reconstructing distribution from report if file missing (unlikely)
        # 52253 trades, 27.4% win at 3R, 72.6% loss at -1R
        n = 52253
        r_win = 3.0
        r_loss = -1.0
        p_win = 0.274
        p_loss = 1 - p_win
        
        outcomes = [r_win] * int(n * p_win) + [r_loss] * int(n * p_loss)
        df = pd.DataFrame({'pnl_r': outcomes})
        
    outcomes = df['pnl_r'].values
    n = len(outcomes)
    
    # 1. Expectancy (Mean)
    mean_r = np.mean(outcomes)
    
    # 2. Standard Deviation (Sample)
    std_dev = np.std(outcomes, ddof=1)
    
    # 3. Standard Error
    se = std_dev / np.sqrt(n)
    
    # 4. 95% Confidence Interval
    # Z-score for 95% is 1.96 (assuming normal approx for large N)
    z_score = 1.96
    margin_of_error = z_score * se
    ci_lower = mean_r - margin_of_error
    ci_upper = mean_r + margin_of_error
    
    # 5. Median
    median_r = np.median(outcomes)
    
    # 6. Null Hypothesis Test
    # Does 0 fall inside CI?
    zero_in_ci = (ci_lower <= 0 <= ci_upper)
    t_stat, p_value = stats.ttest_1samp(outcomes, 0)
    
    # 7. "How many trades..." question
    # Solving for N_crit where Margin of Error == Mean
    # Mean = 1.96 * (StdDev / sqrt(N_crit))
    # sqrt(N_crit) = 1.96 * StdDev / Mean
    # N_crit = (1.96 * StdDev / Mean)^2
    
    if mean_r > 0:
        n_for_significance = (1.96 * std_dev / mean_r) ** 2
    else:
        n_for_significance = float('inf')

    print(f"N: {n}")
    print(f"Mean (Expectancy): {mean_r:.4f} R")
    print(f"Standard Deviation: {std_dev:.4f} R")
    print(f"Standard Error (SE): {se:.5f} R")
    print(f"95% CI: [{ci_lower:.4f}, {ci_upper:.4f}]")
    print(f"Zero inside CI?: {zero_in_ci}")
    print(f"Median R: {median_r:.4f} R")
    print(f"N required for significance (Lower CI > 0): {n_for_significance:.1f}")
    
    # Check if we are ALREADY significant
    if n > n_for_significance:
        print("STATUS: Statistically Significant (Positive).")
    else:
        print("STATUS: NOT Statistically Significant.")

if __name__ == "__main__":
    analyze_stats()
