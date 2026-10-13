# Data Dictionary

This document defines the fields used across all modules.
Field names will be finalised after inspecting the actual ACN-Data dataset.

## Historical Session Record (Batch)

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| session_id | string | ACN-Data | Unique identifier for a charging session |
| station_id | string | ACN-Data | Identifier of the charging station |
| *...* | *...* | *ACN-Data* | *Additional fields TBD after dataset inspection* |

## Streaming Event Record

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| event_id | string | Generated | Unique event identifier |
| station_id | string | Generated | Station identifier |
| event_type | string | Generated | One of: session_start, session_end, energy_update |
| timestamp | ISO 8601 | Generated | Event timestamp (UTC) |
| energy_kwh | float | Generated | Energy delivered so far (kWh) |

---

See `contracts/data_contracts.yaml` for the machine-readable schemas.
