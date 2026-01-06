"""
Pulse Database Migration Runner
Runs all migrations in sequence, handling both fresh and existing databases.

This script:
1. Creates initial schema if tables don't exist (v0.0.1)
2. Adds multi-platform support columns if needed (v0.0.2)

Railway Environment Variable Handling:
- Retries up to 10 times with exponential backoff (max 30s total)
- Provides detailed diagnostics if DATABASE_URL is not available
"""

import os
import sys
import time
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine, text, inspect
from database.models import Base

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


def get_database_url(max_retries: int = 10, initial_delay: float = 0.5) -> str:
    """
    Get database URL from environment with retry logic for Railway deployments.

    Railway may inject environment variables with a slight delay during container startup.
    This function retries with exponential backoff to handle timing issues.

    Args:
        max_retries: Maximum number of retry attempts (default: 10)
        initial_delay: Initial delay in seconds, doubles each retry (default: 0.5s)

    Returns:
        Database URL string

    Raises:
        ValueError: If DATABASE_URL is not available after all retries
    """
    delay = initial_delay

    for attempt in range(max_retries):
        database_url = os.getenv("DATABASE_URL", "")

        if database_url:
            logger.info(f"✅ DATABASE_URL found (attempt {attempt + 1}/{max_retries})")

            # Railway uses 'postgres://' but SQLAlchemy requires 'postgresql://'
            if database_url.startswith("postgres://"):
                database_url = database_url.replace("postgres://", "postgresql://", 1)
                logger.debug("📝 Converted postgres:// to postgresql:// for SQLAlchemy compatibility")

            return database_url

        # DATABASE_URL not found - wait and retry
        if attempt < max_retries - 1:  # Don't wait on last attempt
            logger.warning(
                f"⚠️  DATABASE_URL not set (attempt {attempt + 1}/{max_retries}). "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)
            delay = min(delay * 2, 5.0)  # Exponential backoff, max 5s
        else:
            # Final attempt failed - provide diagnostics
            logger.error("=" * 70)
            logger.error("❌ DATABASE_URL ENVIRONMENT VARIABLE NOT FOUND")
            logger.error("=" * 70)
            logger.error("")
            logger.error("Available environment variables:")
            env_vars = sorted([k for k in os.environ.keys() if not k.startswith('_')])
            for var in env_vars[:20]:  # Show first 20 non-private vars
                logger.error(f"  - {var}")
            if len(env_vars) > 20:
                logger.error(f"  ... and {len(env_vars) - 20} more")
            logger.error("")
            logger.error("Troubleshooting:")
            logger.error("1. Verify DATABASE_URL is set in Railway environment variables")
            logger.error("2. Check Railway service settings for PostgreSQL plugin")
            logger.error("3. Ensure the database service is running")
            logger.error("4. Review Railway deployment logs for database provisioning")
            logger.error("")

            raise ValueError(
                "DATABASE_URL environment variable is not set after "
                f"{max_retries} attempts. See logs above for diagnostics."
            )

    # Should never reach here, but added for type safety
    raise ValueError("Unexpected error in get_database_url retry logic")


def table_exists(engine, table_name: str) -> bool:
    """Check if a table exists in the database."""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def run_migrations():
    """Run all database migrations."""

    database_url = get_database_url()
    logger.info("=" * 70)
    logger.info("PULSE DATABASE MIGRATION - Comprehensive Schema Setup")
    logger.info("=" * 70)
    logger.info(f"📊 Database: {database_url.split('@')[-1] if '@' in database_url else 'local'}")
    logger.info("")

    engine = create_engine(database_url)

    try:
        # =====================================================================
        # STEP 1: Create base schema if tables don't exist (v0.0.1)
        # =====================================================================
        if not table_exists(engine, 'profiles'):
            logger.info("🆕 Fresh database detected - creating initial schema (v0.0.1)")
            logger.info("")
            logger.info("Creating tables from SQLAlchemy models...")

            Base.metadata.create_all(engine)

            logger.info("✅ Initial schema created!")
            logger.info("   Tables: profiles, profile_history, posts, post_history, alert_logs")
            logger.info("")
        else:
            logger.info("✅ Existing database detected - tables already exist")
            logger.info("")

        # =====================================================================
        # STEP 2: Verify all tables are present
        # =====================================================================
        required_tables = ['profiles', 'profile_history', 'posts', 'post_history', 'alert_logs']
        missing_tables = [t for t in required_tables if not table_exists(engine, t)]

        if missing_tables:
            logger.error(f"❌ Missing tables: {missing_tables}")
            logger.error("   Database is in an inconsistent state!")
            return False

        logger.info("✅ All required tables present")
        logger.info("")

        # =====================================================================
        # STEP 3: Verify schema version (check for v0.0.2 columns)
        # =====================================================================
        logger.info("🔍 Checking schema version...")

        with engine.connect() as conn:
            # Check if v0.0.2 migration is needed
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'profiles' AND column_name = 'platform'
            """))

            if result.fetchone():
                logger.info("✅ Schema is up to date (v0.0.2)")
                logger.info("")
                logger.info("=" * 70)
                logger.info("🎉 ALL MIGRATIONS COMPLETE")
                logger.info("=" * 70)
                return True
            else:
                logger.info("⚠️  Schema needs upgrade to v0.0.2")
                logger.info("")

        # =====================================================================
        # STEP 4: Run v0.0.2 migration (add multi-platform columns)
        # =====================================================================
        logger.info("🚀 Applying v0.0.2 migration (Multi-Platform Support)...")
        logger.info("")

        # Import and run the v002 migration
        from database.migrations.v002_multiplatform import run_migration

        success = run_migration()

        if success:
            logger.info("")
            logger.info("=" * 70)
            logger.info("🎉 ALL MIGRATIONS COMPLETE - Database ready!")
            logger.info("=" * 70)
            return True
        else:
            logger.error("❌ Migration v0.0.2 failed")
            return False

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_migrations()
    sys.exit(0 if success else 1)
