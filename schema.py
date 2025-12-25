from enum import Enum
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime

# --- Enums ---

class Session(str, Enum):
    ASIA = "ASIA"
    LONDON = "LONDON"
    NY_AM = "NY_AM"
    NY_PM = "NY_PM"
    OTHER = "OTHER"

class Direction(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

class EventType(str, Enum):
    SWING_HIGH = "SWING_HIGH"
    SWING_LOW = "SWING_LOW"
    COMPRESSION = "COMPRESSION"
    DISPLACEMENT = "DISPLACEMENT"
    LIQUIDITY_SWEEP = "LIQUIDITY_SWEEP"
    LIQUIDITY_SWEEP = "LIQUIDITY_SWEEP"
    FVG = "FVG"
    SESSION_HIGH_SWEEP = "SESSION_HIGH_SWEEP"
    SESSION_LOW_SWEEP = "SESSION_LOW_SWEEP"
    FAILED_BREAKOUT = "FAILED_BREAKOUT"

class Regime(str, Enum):
    LOW_VOL = "LOW_VOL"
    EXPANSION = "EXPANSION"
    CHOP = "CHOP"

# --- Base Structures ---

class ContextTags(BaseModel):
    session: Session
    regime: Regime
    time_of_day: str  # HH:MM
    day_of_week: int
    distance_to_vwap_std: float # Distance in std devs

class StructureEvent(BaseModel):
    """
    Base class for all market structure events.
    These are FACTS extracted from price action, not signals.
    """
    id: str = Field(..., description="Unique ID for the event instance")
    event_type: EventType
    start_bar: datetime
    end_bar: datetime
    confirmed_at: datetime = Field(..., description="The exact timestamp when this event became known structure. CRITICAL for lookahead prevention.")
    direction: Direction
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    context: ContextTags
    metadata: Dict[str, Any] = Field(default_factory=dict)

# --- Concrete Events ---

class SwingEvent(StructureEvent):
    price_level: float
    is_major: bool = False
    tested_count: int = 0

class CompressionEvent(StructureEvent):
    bar_count: int
    average_true_range: float
    compression_ratio: float # e.g., current_range / avg_range

class DisplacementEvent(StructureEvent):
    magnitude_atr: float # Magnitude in ATR multiples
    volume_z_score: float
    closing_price: float

class LiquiditySweepEvent(StructureEvent):
    swept_event_id: str # ID of the Swing/Level that was swept
    sweep_depth_ticks: float
    reclaim_time_bars: int
    
class FailedBreakoutEvent(StructureEvent):
    level_price: float
    failure_mode: str # e.g., "IMMEDIATE_REVERSAL", "GRIND_BACK"

# --- Reasoning Layer ---

class MarketState(BaseModel):
    """
    Represents the complete state of the market at a single timestamp.
    The LLM/Engine reasons over this graph node.
    """
    timestamp: datetime
    session: Session
    regime: Regime
    
    # Active Structure
    active_swings: List[SwingEvent] # Swings not yet violated
    recent_events: List[StructureEvent] # Log of recent completed events (last N bars)
    
    # Relationships
    price_relation_to_vwap: str # "ABOVE", "BELOW", "TOUCHING"
    nearest_support: Optional[float]
    nearest_resistance: Optional[float]

# --- Hypothesis Definitions ---

class Condition(BaseModel):
    metric: str # e.g., "recent_events"
    operator: str # "contains_type", ">", "<"
    value: Any # "LiquiditySweepEvent" or numeric

class Trigger(BaseModel):
    event_type: EventType
    conditions: List[Condition]

class ResultExpectation(BaseModel):
    target_metric: str # "max_excursion_atr"
    min_value: float
    within_bars: int

class InvalidationCriteria(BaseModel):
    metric: str # "price"
    operator: str # "<"
    reference_value: str # "trigger_low"

class Hypothesis(BaseModel):
    """
    Machine-testable hypothesis object.
    Example: H-017 (Sweep + Compression -> Expansion)
    """
    id: str
    description: str
    
    # PRE-CONDITIONS (The generic setup)
    conditions: List[Condition] 
    
    # TRIGGER (The specific entry signal)
    trigger: Trigger
    
    # OUTCOME (What we bet on)
    expectation: ResultExpectation
    
    # INVALIDATION (When we are wrong)
    invalidation: InvalidationCriteria

    # Metadata
    author: str = "AntiGravity"
    status: str = "ACTIVE" # ACTIVE, ARCHIVED, PROVEN_FALSE
