"""
migrate.py :: tiny migration runner for realtor-pro-system

Usage:
    python migrate.py                  # apply any pending migrations
    python migrate.py --check          # list pending migrations, don't apply
    python migrate.py --migrations-dir ./migrations

Requires DATABASE_URL - Supabase's *direct Postgres connection string*
(Project Settings -> Database -> Connection string -> URI), NOT the
SUPABASE_URL/SUPABASE_KEY REST API pair main.py uses. Supabase's REST API
(PostgREST) can't run arbitrary DDL like CREATE TABLE - a real Postgres
connection is required for that, which is exactly what this script is for.

Migrations are plain .sql files in --migrations-dir, applied in filename
order (hence the "0001_", "0002_" prefixes). A schema_migrations table
tracks what's already been applied so re-running this script is a no-op
once everything is up to date.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable, Sequence

from dotenv import load_dotenv

load_dotenv()


def discover_migrations(migrations_dir: Path) -> list[Path]:
    if not migrations_dir.exists():
        return []
    return sorted(p for p in migrations_dir.glob("*.sql") if p.is_file())


def ensure_migrations_table(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename    TEXT PRIMARY KEY,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def get_applied(cursor) -> set[str]:
    cursor.execute("SELECT filename FROM schema_migrations")
    return {row[0] for row in cursor.fetchall()}


def pending_migrations(all_migrations: Sequence[Path], applied: Iterable[str]) -> list[Path]:
    applied_set = set(applied)
    return [m for m in all_migrations if m.name not in applied_set]


def apply_migration(cursor, migration: Path) -> None:
    sql = migration.read_text()
    cursor.execute(sql)
    cursor.execute(
        "INSERT INTO schema_migrations (filename) VALUES (%s) ON CONFLICT DO NOTHING",
        (migration.name,),
    )


def run(migrations_dir: Path, database_url: str, check_only: bool = False) -> int:
    all_migrations = discover_migrations(migrations_dir)
    if not all_migrations:
        print(f"No .sql migrations found in {migrations_dir}")
        return 0

    try:
        import psycopg2
    except ImportError:
        print(
            "psycopg2 is required to actually apply migrations "
            "(pip install psycopg2-binary). Listing discovered migrations instead:\n"
        )
        for m in all_migrations:
            print(f"  - {m.name}")
        return 1

    conn = psycopg2.connect(database_url)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            ensure_migrations_table(cur)
            conn.commit()
            applied = get_applied(cur)

        todo = pending_migrations(all_migrations, applied)

        if not todo:
            print("✅ Database already up to date - nothing to apply.")
            return 0

        print(f"{'Would apply' if check_only else 'Applying'} {len(todo)} migration(s):")
        for m in todo:
            print(f"  - {m.name}")

        if check_only:
            return 0

        with conn.cursor() as cur:
            for m in todo:
                print(f"Applying {m.name}...")
                apply_migration(cur, m)
            conn.commit()

        print("✅ All migrations applied successfully.")
        return 0

    except Exception as e:
        conn.rollback()
        print(f"❌ Migration failed, rolled back: {e}")
        return 2
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply SQL migrations to the Supabase/Postgres database")
    parser.add_argument("--migrations-dir", default="migrations", help="Directory of .sql migration files")
    parser.add_argument("--check", action="store_true", help="List pending migrations without applying them")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Postgres connection string (defaults to DATABASE_URL env var)",
    )
    args = parser.parse_args()

    migrations_dir = Path(args.migrations_dir)

    if not args.database_url:
        print(
            "DATABASE_URL is not set. This is Supabase's direct Postgres connection "
            "string (Project Settings -> Database -> Connection string -> URI), "
            "different from SUPABASE_URL/SUPABASE_KEY used elsewhere.\n"
            "Listing discovered migrations instead of applying them:\n"
        )
        for m in discover_migrations(migrations_dir):
            print(f"  - {m.name}")
        sys.exit(1)

    sys.exit(run(migrations_dir, args.database_url, check_only=args.check))


if __name__ == "__main__":
    main()
