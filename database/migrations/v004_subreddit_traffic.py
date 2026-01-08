"""
Pulse Database Migration - v0.0.4 Subreddit Traffic Analytics

This migration adds the subreddit_traffic table for storing daily Reddit
traffic statistics fetched via PRAW.

Changes:
- Creates subreddit_traffic table with columns for daily traffic metrics
- Adds unique constraint on timestamp to prevent duplicate records
- Supports upsert pattern (ON CONFLICT) for idempotent syncs

Usage:
    python -m database.migrations.v004_subreddit_traffic

Environment Variables:
    DATABASE_URL - PostgreSQL connection string
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


def get_database_url() -> str:
    """Get database URL from environment."""
    database_url = os.getenv("DATABASE_URL", "")

    if not database_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set. "
            "Please set your PostgreSQL connection string."
        )

    # Railway/Heroku use 'postgres://' but SQLAlchemy requires 'postgresql://'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    return database_url


def run_migration():
    """Execute the database migration."""

    try:
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker
    except ImportError:
        logger.error("SQLAlchemy not installed. Run: pip install sqlalchemy psycopg2-binary")
        sys.exit(1)

    database_url = get_database_url()
    logger.info("🔄 Starting migration v0.0.4 - Subreddit Traffic Analytics")
    logger.info(f"📊 Database: {database_url.split('@')[-1] if '@' in database_url else 'local'}")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Check if table already exists
        logger.info("📋 Checking current database state...")

        result = session.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'subreddit_traffic'
        """))

        if result.fetchone():
            logger.info("✅ Migration already applied (subreddit_traffic table exists)")
            return True

        logger.info("🚀 Applying migration...")

        # =================================================================
        # STEP 1: Create subreddit_traffic table
        # =================================================================
        logger.info("  [1/2] Creating subreddit_traffic table...")

        session.execute(text("""
            CREATE TABLE IF NOT EXISTS subreddit_traffic (
                id SERIAL PRIMARY KEY,
                subreddit_name VARCHAR(128) NOT NULL,
                timestamp DATE NOT NULL,
                unique_visitors INTEGER DEFAULT 0,
                pageviews INTEGER DEFAULT 0,
                subscriptions INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_subreddit_traffic_date UNIQUE (subreddit_name, timestamp)
            )
        """))

        session.commit()

        # =================================================================
        # STEP 2: Create indexes for efficient queries
        # =================================================================
        logger.info("  [2/2] Creating indexes...")

        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_subreddit_traffic_subreddit
            ON subreddit_traffic(subreddit_name)
        """))

        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_subreddit_traffic_timestamp
            ON subreddit_traffic(timestamp DESC)
        """))

        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_subreddit_traffic_lookup
            ON subreddit_traffic(subreddit_name, timestamp DESC)
        """))

        session.commit()

        # =================================================================
        # VERIFY MIGRATION
        # =================================================================
        logger.info("📊 Verifying migration...")

        result = session.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'subreddit_traffic'
            ORDER BY ordinal_position
        """))

        columns = result.fetchall()
        logger.info(f"   ✅ Table created with {len(columns)} columns:")
        for col in columns:
            logger.info(f"      - {col[0]} ({col[1]})")

        logger.info("🎉 Migration v0.0.4 completed successfully!")

        return True

    except Exception as e:
        session.rollback()
        logger.error(f"❌ Migration failed: {e}")
        raise
    finally:
        session.close()


def rollback_migration():
    """Rollback the migration (for development/testing only)."""

    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    database_url = get_database_url()
    logger.warning("⚠️ Rolling back migration v0.0.4...")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        session.execute(text("DROP TABLE IF EXISTS subreddit_traffic CASCADE"))
        session.commit()
        logger.info("✅ Rollback completed - subreddit_traffic table dropped")

    except Exception as e:
        session.rollback()
        logger.error(f"❌ Rollback failed: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pulse Database Migration v0.0.4")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    args = parser.parse_args()

    if args.rollback:
        rollback_migration()
    else:
        run_migration()
