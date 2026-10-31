"""
database/connection.py — PostgreSQL connection management and migration runner.

Owner: Bainty Kaur (database/), Aleena Antony (processing/)

Provides secure database connectivity, configuration extraction with masked credentials,
connectivity health checks, and transactional SQL schema migration execution.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("database.connection")

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import connection as PgConnection
    PSYCOPG2_AVAILABLE = True
except ImportError:
    psycopg2 = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]
    PgConnection = Any  # type: ignore[misc, assignment]
    PSYCOPG2_AVAILABLE = False


def get_db_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Retrieve database configuration from environment variables or overrides.
    Never prints or logs passwords.
    """
    conf = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "ev_charging"),
        "user": os.getenv("POSTGRES_USER", "ev_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "ev_password"),
        "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "5")),
    }
    if overrides:
        conf.update(overrides)
    return conf


def get_masked_db_url(config: dict[str, Any] | None = None) -> str:
    """Return safe PostgreSQL connection string with password masked."""
    cfg = config or get_db_config()
    return f"postgresql://{cfg['user']}:***@{cfg['host']}:{cfg['port']}/{cfg['database']}"


def get_db_connection(config: dict[str, Any] | None = None) -> PgConnection:
    """
    Establish and return a psycopg2 database connection.
    Raises RuntimeError if psycopg2 is missing or connection fails.
    """
    if not PSYCOPG2_AVAILABLE:
        raise RuntimeError("psycopg2-binary is required for PostgreSQL connectivity.")

    cfg = config or get_db_config()
    try:
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            connect_timeout=cfg.get("connect_timeout", 5),
        )
        return conn
    except Exception as exc:
        logger.error("Failed to connect to PostgreSQL at %s: %s", get_masked_db_url(cfg), exc)
        raise


def check_db_connectivity(config: dict[str, Any] | None = None, timeout: int = 3) -> bool:
    """
    Check if the target PostgreSQL database is reachable and accepting queries.
    """
    if not PSYCOPG2_AVAILABLE:
        return False

    cfg = dict(config or get_db_config())
    cfg["connect_timeout"] = timeout
    try:
        conn = get_db_connection(cfg)
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        conn.close()
        return True
    except Exception:
        return False


def run_migrations(
    migrations_dir: Path | str | None = None,
    config: dict[str, Any] | None = None,
) -> list[str]:
    """
    Execute pending SQL migrations in ascending order inside transactions.

    Returns
    -------
    list[str]
        List of applied migration filenames.
    """
    if migrations_dir is None:
        target_dir = PROJECT_ROOT / "database" / "migrations"
    else:
        target_dir = Path(migrations_dir)

    if not target_dir.exists():
        logger.warning("Migrations directory does not exist: %s", target_dir)
        return []

    migration_files = sorted(target_dir.glob("*.sql"))
    if not migration_files:
        logger.info("No SQL migration files found in %s", target_dir)
        return []

    conn = get_db_connection(config)
    applied: list[str] = []

    try:
        # Ensure migration tracking table exists
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(64) PRIMARY KEY,
                    description TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

        # Query already applied migrations
        with conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_migrations;")
            rows = cur.fetchall()
            applied_versions = {r[0] for r in rows}

        for mig in migration_files:
            version_name = mig.stem  # e.g. '001_initial_schema'
            if version_name in applied_versions:
                logger.debug("Migration already applied: %s", version_name)
                continue

            logger.info("Applying migration: %s ...", mig.name)
            sql_content = mig.read_text(encoding="utf-8")

            with conn.cursor() as cur:
                cur.execute(sql_content)
                cur.execute(
                    "INSERT INTO schema_migrations (version, description) VALUES (%s, %s) ON CONFLICT (version) DO NOTHING;",
                    (version_name, f"Applied from {mig.name}"),
                )
            conn.commit()
            applied.append(version_name)
            logger.info("Migration '%s' applied successfully.", version_name)

        return applied
    except Exception as exc:
        conn.rollback()
        logger.error("Migration failed, transaction rolled back: %s", exc)
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="EV Charging PostgreSQL Connection & Migrations")
    parser.add_argument("--check", action="store_true", help="Check database connectivity and exit")
    parser.add_argument("--migrate", action="store_true", help="Execute pending schema migrations")
    args = parser.parse_args()

    cfg = get_db_config()
    logger.info("Configured target: %s", get_masked_db_url(cfg))

    if args.check:
        is_ok = check_db_connectivity(cfg)
        if is_ok:
            logger.info("PostgreSQL is reachable and healthy.")
            sys.exit(0)
        else:
            logger.error("PostgreSQL is NOT reachable.")
            sys.exit(1)

    if args.migrate:
        applied = run_migrations(config=cfg)
        logger.info("Applied %d migration(s): %s", len(applied), applied)


if __name__ == "__main__":
    main()
