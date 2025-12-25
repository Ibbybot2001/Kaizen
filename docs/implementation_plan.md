# Implementation Plan - Structural Hypothesis Backtesting Engine (SHBE)

## Goal Description
Build a closed-loop research engine for testing market structure hypotheses on 1-minute IBKR data. The system utilizes a strict schema for market events, allowing for deterministic backtesting and LLM-driven hypothesis evolution.

## User Review Required
- **GitHub Repository**: User needs to provide the remote URL to push the local repository.
- **Data Source**: Confirm `kaizen_1m_data_ibkr_2yr.csv` is the canonical source in the parent directory.

## Proposed Changes

### Core Infrastructure
#### [NEW] [schema.py](file:///C:/Users/CEO/.gemini/antigravity/scratch/shbe/schema.py)
- `StructureEvent` (Base Pydantic model)
- `SwingEvent`, `CompressionEvent`, `DisplacementEvent`, `LiquiditySweepEvent`
- `MarketState` (Graph node definition)
- `Hypothesis` (Template for Logic)

#### [NEW] [data_loader.py](file:///C:/Users/CEO/.gemini/antigravity/scratch/shbe/data_loader.py)
- Polars/Pandas loading of IBKR CSV 
- Immutable data enforcement
- Session indexing (Asia/London/NY)
- Rolling window features (ATR, VWAP)

#### [NEW] [engine.py](file:///C:/Users/CEO/.gemini/antigravity/scratch/shbe/engine.py)
- Deterministic backtesting logic
- Hypothesis evaluation loop

### Components structure
- **Data Layer**: cleaning, normalizing, indexing
- **Extraction Layer**: identifying discrete market events
- **Reasoning Layer**: matching states to hypotheses
- **Execution Layer**: simulating outcomes

## Verification Plan

### Automated Tests
- Unit tests for each Event Extractor (e.g., ensure Swings are identified correctly on synthetic data).
- Consistency check: Ensure `MarketState` is valid for every timestamp.

### Manual Verification
- Visual validation of extracted structure against a sample chart segment (using `matplotlib` or similar if needed, or just data inspection).
