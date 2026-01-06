"""
Database Reset Utility - Clean Slate for Fresh Start

This utility deletes all data from the database while preserving the schema structure.
Use this when you need to start fresh after schema changes or data corruption.

CAUTION: This will permanently delete all profiles, posts, history, and alerts!

Usage:
    # From code (e.g., app.py)
    from database.reset_database import reset_all_data
    success = reset_all_data()

    # From command line
    python -m database.reset_database

    # With confirmation prompt
    python -m database.reset_database --confirm
"""

import os
import sys
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

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

    # Railway uses 'postgres://' but SQLAlchemy requires 'postgresql://'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    return database_url


def reset_all_data(confirm: bool = False) -> bool:
    """
    Delete all data from all tables while preserving schema.

    Args:
        confirm: If True, skip confirmation prompt (use with caution!)

    Returns:
        True if successful, False otherwise
    """
    if not confirm:
        logger.warning("=" * 70)
        logger.warning("⚠️  DATABASE RESET - ALL DATA WILL BE DELETED")
        logger.warning("=" * 70)
        logger.warning("")
        logger.warning("This will permanently delete:")
        logger.warning("  - All tracked profiles")
        logger.warning("  - All posts and performance data")
        logger.warning("  - All historical metrics")
        logger.warning("  - All alert logs")
        logger.warning("")
        logger.warning("The database schema will be preserved (tables, columns, constraints).")
        logger.warning("")

        response = input("Are you sure you want to continue? Type 'YES' to confirm: ")

        if response != "YES":
            logger.info("❌ Reset cancelled by user")
            return False

    try:
        database_url = get_database_url()
        logger.info("🔄 Starting database reset...")
        logger.info(f"📊 Database: {database_url.split('@')[-1] if '@' in database_url else 'local'}")
        logger.info("")

        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Disable foreign key checks temporarily (for PostgreSQL)
            # Note: PostgreSQL doesn't have a global FK disable, so we use CASCADE

            # =====================================================================
            # Delete in correct order to respect foreign key constraints
            # =====================================================================

            logger.info("  [1/5] Deleting alert logs...")
            result = session.execute(text("DELETE FROM alert_logs"))
            alert_count = result.rowcount
            session.commit()
            logger.info(f"     ✅ Deleted {alert_count} alert logs")

            logger.info("  [2/5] Deleting post history...")
            result = session.execute(text("DELETE FROM post_history"))
            post_history_count = result.rowcount
            session.commit()
            logger.info(f"     ✅ Deleted {post_history_count} post history records")

            logger.info("  [3/5] Deleting posts...")
            result = session.execute(text("DELETE FROM posts"))
            posts_count = result.rowcount
            session.commit()
            logger.info(f"     ✅ Deleted {posts_count} posts")

            logger.info("  [4/5] Deleting profile history...")
            result = session.execute(text("DELETE FROM profile_history"))
            profile_history_count = result.rowcount
            session.commit()
            logger.info(f"     ✅ Deleted {profile_history_count} profile history records")

            logger.info("  [5/5] Deleting profiles...")
            result = session.execute(text("DELETE FROM profiles"))
            profiles_count = result.rowcount
            session.commit()
            logger.info(f"     ✅ Deleted {profiles_count} profiles")

            # Reset sequences (auto-increment counters)
            logger.info("")
            logger.info("  [6/6] Resetting ID sequences...")
            session.execute(text("ALTER SEQUENCE profiles_id_seq RESTART WITH 1"))
            session.execute(text("ALTER SEQUENCE profile_history_id_seq RESTART WITH 1"))
            session.execute(text("ALTER SEQUENCE posts_id_seq RESTART WITH 1"))
            session.execute(text("ALTER SEQUENCE post_history_id_seq RESTART WITH 1"))
            session.execute(text("ALTER SEQUENCE alert_logs_id_seq RESTART WITH 1"))
            session.commit()
            logger.info("     ✅ All ID sequences reset to 1")

            logger.info("")
            logger.info("=" * 70)
            logger.info("🎉 DATABASE RESET COMPLETE")
            logger.info("=" * 70)
            logger.info("")
            logger.info(f"Total records deleted:")
            logger.info(f"  - Profiles: {profiles_count}")
            logger.info(f"  - Posts: {posts_count}")
            logger.info(f"  - Profile History: {profile_history_count}")
            logger.info(f"  - Post History: {post_history_count}")
            logger.info(f"  - Alert Logs: {alert_count}")
            logger.info("")
            logger.info("✅ Database is now empty and ready for fresh data")

            return True

        except Exception as e:
            session.rollback()
            logger.error(f"❌ Reset failed: {e}")
            raise

        finally:
            session.close()

    except Exception as e:
        logger.error(f"❌ Database reset failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Reset database - delete all data")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt (dangerous!)"
    )
    args = parser.parse_args()

    success = reset_all_data(confirm=args.confirm)
    sys.exit(0 if success else 1)
