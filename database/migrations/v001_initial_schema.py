"""
Pulse Database Migration - v0.0.1 Initial Schema
Creates all base tables for the multi-platform analytics dashboard.

This migration creates the foundation schema with support for TikTok, Twitter/X, and Reddit.
"""

import os
import sys
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine
from database.models import Base

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


def run_initial_migration():
    """Create all base tables using SQLAlchemy models."""

    database_url = get_database_url()
    logger.info("=" * 60)
    logger.info("Creating Initial Database Schema (v0.0.1)")
    logger.info("=" * 60)
    logger.info(f"Database: {database_url.split('@')[-1] if '@' in database_url else 'local'}")
    logger.info("")

    engine = create_engine(database_url)

    try:
        # Create all tables defined in models
        logger.info("Creating tables from SQLAlchemy models...")
        Base.metadata.create_all(engine)

        logger.info("✅ Initial schema created successfully!")
        logger.info("")
        logger.info("Tables created:")
        logger.info("  - profiles")
        logger.info("  - profile_history")
        logger.info("  - posts")
        logger.info("  - post_history")
        logger.info("  - alert_logs")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"❌ Schema creation failed: {e}")
        raise


if __name__ == "__main__":
    run_initial_migration()
