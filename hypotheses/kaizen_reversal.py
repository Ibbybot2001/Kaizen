from schema import Hypothesis, Condition, Trigger, ResultExpectation, InvalidationCriteria, EventType, Regime, Direction

def get_kaizen_reversal_hypothesis() -> Hypothesis:
    """
    Returns the Hypothesis object for the Kaizen Reversal Strategy.
    Logic:
      - Trigger: Liquidity Sweep (Price takes out a pivot but closes back inside).
      - Direction: Implied by sweep type (Sweep Low -> Bullish, Sweep High -> Bearish).
      - Expectation: 3.0 R (from user input tpMult=3.0).
      - Invalidation: Price closes beyond the sweep wick (Standard invalidation).
    """
    return Hypothesis(
        id="KAIZEN-REV-001",
        description="Kaizen Reversal: Liquidity Sweep + Reclaim targeting 3R",
        conditions=[
            # Strategy is generally valid in all regimes, but best in CHOP or Counter-Trend.
            # For now, no strict pre-conditions other than the structure itself.
        ],
        trigger=Trigger(
            event_type=EventType.LIQUIDITY_SWEEP, 
            conditions=[] 
        ),
        expectation=ResultExpectation(
            target_metric="r_multiple", 
            min_value=3.0, 
            within_bars=60 # Give it an hour to work? Cooldown is small.
        ),
        invalidation=InvalidationCriteria(
            metric="price", 
            operator="<", 
            reference_value="trigger_low" # For long. Engine flips this for short.
        )
    )
