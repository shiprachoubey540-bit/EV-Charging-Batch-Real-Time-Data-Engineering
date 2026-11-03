# ER Diagram (PostgreSQL warehouse)

Real historical data: dim_station, charging_sessions, session_user_inputs.
Simulated streaming data: charging_events, station_window_metrics.

```mermaid
erDiagram
    dim_station ||--o{ charging_sessions : "has sessions"
    charging_sessions ||--o{ session_user_inputs : "has inputs"
    dim_station ||..o{ charging_events : "emits (no FK)"
    dim_station ||..o{ station_window_metrics : "aggregated (no FK)"

    dim_station {
        varchar station_id PK
        varchar space_id
        varchar site_id
        varchar cluster_id
    }
    charging_sessions {
        varchar id PK
        varchar session_id UK
        varchar station_id FK
        timestamptz connection_time
        timestamptz disconnect_time
        timestamptz done_charging_time
        double kwh_delivered
        varchar user_id
        double session_duration_min
        double charging_duration_min
        double idle_duration_min
        text validation_flags
    }
    session_user_inputs {
        serial id PK
        varchar session_id FK
        bigint user_id_numeric
        double kwh_requested
        double minutes_available
        timestamptz requested_departure
        boolean payment_required
    }
    charging_events {
        varchar event_id PK
        varchar station_id
        varchar session_id
        varchar event_type
        timestamptz event_timestamp
        double energy_kwh
        double incremental_kwh
        boolean is_synthetic
    }
    station_window_metrics {
        serial id PK
        varchar station_id
        timestamptz window_start
        timestamptz window_end
        bigint event_count
        double total_incremental_kwh
        double max_cumulative_kwh
        boolean is_synthetic
    }
```

Solid lines are enforced foreign keys. Dotted lines are logical links only: simulated
tables deliberately have no foreign key, so a wrong simulated station cannot crash the
stream. Quality check Q11 detects such rows.