"""
Pulse Database Migration - v0.0.3 Increase ID Column Lengths

This migration increases the size of ID columns to accommodate longer identifiers
from the Lundehund tiktok-api23 API (secUid values are longer than 64 characters).

Changes:
- Increase profiles.platform_user_id from VARCHAR(64) to VARCHAR(255)
- Increase profiles.tiktok_user_id from VARCHAR(64) to VARCHAR(255)
- Increase posts.platform_post_id from VARCHAR(64) to VARCHAR(255)
- Increase posts.tiktok_post_id from VARCHAR(64) to VARCHAR(255)

Why: The Lundehund API returns secUid values that can exceed 64 characters,
causing "value too long for type character varying(64)" errors in PostgreSQL.

Usage:
    python -m database.migrations.v003_increase_id_lengths

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
    logger.info("🔄 Starting migration v0.0.3 - Increase ID Column Lengths")
    logger.info(f"📊 Database: {database_url.split('@')[-1] if '@' in database_url else 'local'}")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Check current state
        logger.info("📋 Checking current database state...")

        # Check if columns already have the correct size
        result = session.execute(text("""
            SELECT character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'profiles'
            AND column_name = 'platform_user_id'
        """))

        current_length = result.scalar()

        if current_length and current_length >= 255:
            logger.info("✅ Migration already applied (platform_user_id is already VARCHAR(255) or larger)")
            return True

        logger.info(f"📏 Current platform_user_id length: {current_length}")
        logger.info("🚀 Applying migration...")

        # =================================================================
        # STEP 1: Increase profiles table ID column sizes
        # =================================================================
        logger.info("  [1/4] Increasing profiles.platform_user_id to VARCHAR(255)...")

        session.execute(text("""
            ALTER TABLE profiles
            ALTER COLUMN platform_user_id TYPE VARCHAR(255)
        """))

        session.commit()

        # =================================================================
        # STEP 2: Increase profiles.tiktok_user_id (legacy column)
        # =================================================================
        logger.info("  [2/4] Increasing profiles.tiktok_user_id to VARCHAR(255)...")

        # Check if column exists first (it might not in fresh installs)
        result = session.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'profiles' AND column_name = 'tiktok_user_id'
        """))

        if result.fetchone():
            session.execute(text("""
                ALTER TABLE profiles
                ALTER COLUMN tiktok_user_id TYPE VARCHAR(255)
            """))
            session.commit()
            logger.info("     ✅ tiktok_user_id column updated")
        else:
            logger.info("     ℹ️  tiktok_user_id column doesn't exist (skipping)")

        # =================================================================
        # STEP 3: Increase posts.platform_post_id
        # =================================================================
        logger.info("  [3/4] Increasing posts.platform_post_id to VARCHAR(255)...")

        session.execute(text("""
            ALTER TABLE posts
            ALTER COLUMN platform_post_id TYPE VARCHAR(255)
        """))

        session.commit()

        # =================================================================
        # STEP 4: Increase posts.tiktok_post_id (legacy column)
        # =================================================================
        logger.info("  [4/4] Increasing posts.tiktok_post_id to VARCHAR(255)...")

        # Check if column exists first
        result = session.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'posts' AND column_name = 'tiktok_post_id'
        """))

        if result.fetchone():
            session.execute(text("""
                ALTER TABLE posts
                ALTER COLUMN tiktok_post_id TYPE VARCHAR(255)
            """))
            session.commit()
            logger.info("     ✅ tiktok_post_id column updated")
        else:
            logger.info("     ℹ️  tiktok_post_id column doesn't exist (skipping)")

        # =================================================================
        # VERIFY MIGRATION
        # =================================================================
        logger.info("📊 Verifying migration...")

        # Verify all columns are now 255
        result = session.execute(text("""
            SELECT
                column_name,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_name IN ('profiles', 'posts')
            AND column_name IN ('platform_user_id', 'tiktok_user_id', 'platform_post_id', 'tiktok_post_id')
            ORDER BY table_name, column_name
        """))

        columns = result.fetchall()
        for column_name, max_length in columns:
            if max_length == 255:
                logger.info(f"   ✅ {column_name}: VARCHAR({max_length})")
            else:
                logger.warning(f"   ⚠️  {column_name}: VARCHAR({max_length}) (expected 255)")

        logger.info("🎉 Migration v0.0.3 completed successfully!")
        logger.info("   All ID columns can now store secUid values up to 255 characters")

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
    logger.warning("⚠️ Rolling back migration v0.0.3...")
    logger.warning("⚠️ WARNING: This will truncate ID columns back to VARCHAR(64)")
    logger.warning("⚠️ Any existing values longer than 64 characters will cause errors!")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Reduce column sizes back to 64
        logger.info("  [1/4] Reducing profiles.platform_user_id to VARCHAR(64)...")
        session.execute(text("""
            ALTER TABLE profiles
            ALTER COLUMN platform_user_id TYPE VARCHAR(64)
        """))

        logger.info("  [2/4] Reducing profiles.tiktok_user_id to VARCHAR(64)...")
        result = session.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'profiles' AND column_name = 'tiktok_user_id'
        """))
        if result.fetchone():
            session.execute(text("""
                ALTER TABLE profiles
                ALTER COLUMN tiktok_user_id TYPE VARCHAR(64)
            """))

        logger.info("  [3/4] Reducing posts.platform_post_id to VARCHAR(64)...")
        session.execute(text("""
            ALTER TABLE posts
            ALTER COLUMN platform_post_id TYPE VARCHAR(64)
        """))

        logger.info("  [4/4] Reducing posts.tiktok_post_id to VARCHAR(64)...")
        result = session.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'posts' AND column_name = 'tiktok_post_id'
        """))
        if result.fetchone():
            session.execute(text("""
                ALTER TABLE posts
                ALTER COLUMN tiktok_post_id TYPE VARCHAR(64)
            """))

        session.commit()
        logger.info("✅ Rollback completed")

    except Exception as e:
        session.rollback()
        logger.error(f"❌ Rollback failed: {e}")
        logger.error("This is likely because existing data exceeds 64 characters")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pulse Database Migration v0.0.3")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    args = parser.parse_args()

    if args.rollback:
        rollback_migration()
    else:
        run_migration()
