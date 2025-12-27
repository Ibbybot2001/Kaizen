# 🧠 KAIZEN ENGINE — NON-NEGOTIABLES & CONSTITUTION

**(This document governs all future work on the Kaizen Engine)**

---

## 0️⃣ PURPOSE (WHY THIS EXISTS)

This repository exists to build **ONE canonical Kaizen Engine** whose behavior is:

* **Structure-first**
* **Mode-driven**
* **Pine Script authoritative**
* **Python = faithful semantic twin**
* **Frequency is a consequence, not a goal**

Any work that violates this purpose is invalid.

---

## 1️⃣ PRIMARY OBJECTIVES (IMMUTABLE)

These objectives **cannot be re-interpreted** or reordered.

### 🎯 OBJECTIVE 1 — STRUCTURE BEFORE PERFORMANCE

We are **not optimizing profitability** until structure behavior is fully understood.

Success is measured by:

* correct structure detection
* correct lifecycle handling
* explainable failures
* consistent signal semantics across modes

Profitability comes **later**.

---

### 🎯 OBJECTIVE 2 — ONE ENGINE, MANY MODES

There is **exactly one trading engine**.

Modes are **filters layered on top**, not separate strategies.

Allowed modes:

* **Sampling Mode** (Discovery)
* **Strict Mode** (Execution)
* **Ultra-Strict Mode** (Context - Future)

Disallowed:

* parallel strategies
* “strict-only” logic
* Python-only improvements

---

### 🎯 OBJECTIVE 3 — PINE SCRIPT IS SOURCE OF TRUTH

Pine Script defines:

* timing
* causality
* structure semantics
* lifecycle rules

Python **must replicate**, not reinterpret.

If Pine and Python disagree:

> **Python is wrong until proven otherwise.**

---

### 🎯 OBJECTIVE 4 — FREQUENCY IS A DIAL, NOT A TARGET

Trade frequency is **not minimized by default**.

Target ranges:

* **Sampling:** ~10–20 trades/day
* **Strict:** ~1–3 trades/week
* **Ultra-Strict:** ~1–3 trades/month

These are **configuration outcomes**, not optimization goals.

---

## 2️⃣ ABSOLUTE ENGINE RULES (NON-NEGOTIABLE)

These rules **cannot be changed without explicit repo-level approval**.

---

### RULE A — STRUCTURE IS MODE-INVARIANT

The following must be **identical across all modes**:

* pivot detection
* provisional → confirmed promotion
* reclaim definition
* stop placement logic
* signal eligibility rules

❌ Modes must NEVER alter structure
✅ Modes may ONLY block or allow execution

---

### RULE B — MODES ONLY ADD GATES

Modes may ONLY:

* block trades
* delay trades
* limit usage
* enforce cooldowns

Modes may NOT:

* change entry logic
* redefine reclaim
* alter pivots
* introduce new signal types

---

### RULE C — SAMPLING MODE IS SACRED

Sampling Mode exists **only for data discovery**.

Sampling Mode MUST:

* allow repeated tests of the same level
* allow counter-trend trades
* allow mild expansion
* disable usage gating
* disable cooldowns
* disable trend bias

If Sampling Mode becomes “clean” or “selective”, it is broken.

---

### RULE D — STRICT MODE IS A SUBSET, NOT A DIFFERENT BRAIN

Every Strict Mode trade must be:

> A trade that *already existed* in Sampling Mode.

If Strict Mode produces a trade that Sampling Mode never saw:
❌ the engine is invalid.

---

### RULE E — NO PARAMETER TUNING BEFORE MODE PARITY

ATR, volume, thresholds, multipliers **cannot be tuned** until:

1. Sampling Mode Pine ≈ Python
2. Strict Mode Pine ≈ Python
3. Mode transitions are verified

Tuning before parity is **explicitly forbidden**.

---

## 3️⃣ MODE CONTRACT (WHAT EACH MODE IS ALLOWED TO TOUCH)

### 🧪 SAMPLING MODE

Allowed:

* minimal expansion gate
* unlimited retests
* no cooldown
* no usage flags
* no trend filter

Goal:

> “Show me everything the structure *wants* to do.”

---

### 🔒 STRICT MODE

Adds:

* usage gating (one-shot per level)
* structural cooldown
* strong expansion lock

Does NOT add:

* new signals
* new structure
* new bias logic

Goal:

> “Filter noise, not ideas.”

---

## 4️⃣ CHANGE MANAGEMENT RULES

Every change must answer **all three**:

1. **Which rule does this touch?**
2. **Which mode does this affect?**
3. **Does this change structure or only gating?**

If structure is touched:

* Sampling parity must be revalidated
* Strict parity must be revalidated

No exceptions.
