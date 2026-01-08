#!/usr/bin/env python3
"""
Pulse - Reddit Traffic Sync Service

Standalone script for fetching daily Reddit subreddit traffic statistics
using PRAW (Python Reddit API Wrapper). Designed for Railway cron execution.

Usage:
    python -m services.reddit_sync

    # Or with specific subreddit:
    python -m services.reddit_sync --subreddit yoursubreddit

Environment Variables:
    REDDIT_CLIENT_ID     - Reddit API client ID
    REDDIT_CLIENT_SECRET - Reddit API client secret
    REDDIT_USERNAME      - Reddit account username
    REDDIT_PASSWORD      - Reddit account password
    REDDIT_SUBREDDIT     - Default subreddit to track (without r/)
    DATABASE_URL         - PostgreSQL connection string

Railway Cron Schedule:
    0 0 * * * (runs daily at midnight UTC)
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.logger import get_logger, setup_root_logger

# Initialize logging
setup_root_logger()
logger = get_logger(__name__)


def get_database_url() -> str:
    """Get database URL from environment."""
    database_url = os.getenv("DATABASE_URL", "")

    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    # Railway uses 'postgres://' but SQLAlchemy requires 'postgresql://'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    return database_url


def get_reddit_credentials() -> dict:
    """Get Reddit API credentials from environment."""
    required_vars = [
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USERNAME",
        "REDDIT_PASSWORD",
    ]

    credentials = {}
    missing = []

    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing.append(var)
        else:
            credentials[var.lower().replace("reddit_", "")] = value

    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    return credentials


def fetch_subreddit_traffic(subreddit_name: str) -> list[dict]:
    """
    Fetch traffic statistics for a subreddit using PRAW.

    Args:
        subreddit_name: Name of the subreddit (without r/)

    Returns:
        List of dicts with daily traffic data (timestamp, unique_visitors, pageviews)
    """
    try:
        import praw
    except ImportError:
        logger.error("PRAW not installed. Run: pip install praw")
        raise ImportError("praw package is required. Install with: pip install praw")

    credentials = get_reddit_credentials()

    logger.info(f"Connecting to Reddit API for r/{subreddit_name}...")

    # Create Reddit instance
    reddit = praw.Reddit(
        client_id=credentials["client_id"],
        client_secret=credentials["client_secret"],
        username=credentials["username"],
        password=credentials["password"],
        user_agent=f"Pulse Analytics Dashboard by /u/{credentials['username']}",
    )

    # Verify authentication
    try:
        authenticated_user = reddit.user.me()
        logger.info(f"Authenticated as u/{authenticated_user.name}")
    except Exception as e:
        logger.error(f"Reddit authentication failed: {e}")
        raise

    # Get subreddit and fetch traffic
    try:
        subreddit = reddit.subreddit(subreddit_name)

        # Verify we have moderator access (required for traffic stats)
        try:
            traffic = subreddit.traffic()
        except Exception as e:
            logger.error(
                f"Cannot access traffic for r/{subreddit_name}. "
                f"Ensure u/{credentials['username']} is a moderator. Error: {e}"
            )
            raise PermissionError(
                f"Traffic stats require moderator access to r/{subreddit_name}"
            )

        # Parse traffic data
        # traffic() returns dict with 'day', 'hour', 'month' keys
        # 'day' is a list of [timestamp, uniques, pageviews, subscriptions]
        daily_traffic = []

        if "day" in traffic:
            for day_data in traffic["day"]:
                # day_data format: [unix_timestamp, unique_visitors, pageviews, subscriptions]
                if len(day_data) >= 4:
                    timestamp = datetime.utcfromtimestamp(day_data[0])
                    daily_traffic.append({
                        "timestamp": timestamp,
                        "unique_visitors": day_data[1] or 0,
                        "pageviews": day_data[2] or 0,
                        "subscriptions": day_data[3] or 0,
                    })

        logger.info(f"Fetched {len(daily_traffic)} days of traffic data")
        return daily_traffic

    except PermissionError:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch traffic for r/{subreddit_name}: {e}")
        raise


def upsert_traffic_data(subreddit_name: str, traffic_data: list[dict]) -> int:
    """
    Upsert traffic data into the database using ON CONFLICT pattern.

    Args:
        subreddit_name: Name of the subreddit
        traffic_data: List of traffic records

    Returns:
        Number of records upserted
    """
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    database_url = get_database_url()
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    upserted_count = 0

    try:
        for record in traffic_data:
            # Use PostgreSQL upsert (INSERT ... ON CONFLICT)
            session.execute(
                text("""
                    INSERT INTO subreddit_traffic
                        (subreddit_name, timestamp, unique_visitors, pageviews, subscriptions, created_at, updated_at)
                    VALUES
                        (:subreddit_name, :timestamp, :unique_visitors, :pageviews, :subscriptions, NOW(), NOW())
                    ON CONFLICT (subreddit_name, timestamp)
                    DO UPDATE SET
                        unique_visitors = EXCLUDED.unique_visitors,
                        pageviews = EXCLUDED.pageviews,
                        subscriptions = EXCLUDED.subscriptions,
                        updated_at = NOW()
                """),
                {
                    "subreddit_name": subreddit_name,
                    "timestamp": record["timestamp"],
                    "unique_visitors": record["unique_visitors"],
                    "pageviews": record["pageviews"],
                    "subscriptions": record["subscriptions"],
                },
            )
            upserted_count += 1

        session.commit()
        logger.info(f"Upserted {upserted_count} traffic records for r/{subreddit_name}")
        return upserted_count

    except Exception as e:
        session.rollback()
        logger.error(f"Database error during upsert: {e}")
        raise
    finally:
        session.close()


def sync_reddit_traffic(subreddit_name: Optional[str] = None) -> bool:
    """
    Main sync function - fetches and stores Reddit traffic data.

    Args:
        subreddit_name: Subreddit to sync (uses REDDIT_SUBREDDIT env var if not provided)

    Returns:
        True if sync succeeded, False otherwise
    """
    # Get subreddit name
    if not subreddit_name:
        subreddit_name = os.getenv("REDDIT_SUBREDDIT")

    if not subreddit_name:
        logger.error("No subreddit specified. Set REDDIT_SUBREDDIT or use --subreddit")
        return False

    # Remove r/ prefix if present
    subreddit_name = subreddit_name.lstrip("r/").strip()

    logger.info("=" * 60)
    logger.info(f"REDDIT TRAFFIC SYNC - r/{subreddit_name}")
    logger.info(f"Started at: {datetime.utcnow().isoformat()}Z")
    logger.info("=" * 60)

    try:
        # Fetch traffic from Reddit API
        traffic_data = fetch_subreddit_traffic(subreddit_name)

        if not traffic_data:
            logger.warning(f"No traffic data available for r/{subreddit_name}")
            return True  # Not an error, just no data

        # Store in database
        upserted = upsert_traffic_data(subreddit_name, traffic_data)

        logger.info("=" * 60)
        logger.info("SYNC COMPLETE")
        logger.info(f"  Subreddit: r/{subreddit_name}")
        logger.info(f"  Records processed: {upserted}")
        logger.info(f"  Completed at: {datetime.utcnow().isoformat()}Z")
        logger.info("=" * 60)

        return True

    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        return False
    except PermissionError as e:
        logger.error(f"Permission denied: {e}")
        return False
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sync Reddit subreddit traffic statistics to database"
    )
    parser.add_argument(
        "--subreddit",
        "-s",
        type=str,
        help="Subreddit name (without r/). Defaults to REDDIT_SUBREDDIT env var",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't store in database",
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY RUN MODE - Data will be fetched but not stored")
        subreddit = args.subreddit or os.getenv("REDDIT_SUBREDDIT", "").lstrip("r/")

        if not subreddit:
            logger.error("No subreddit specified")
            sys.exit(1)

        try:
            traffic = fetch_subreddit_traffic(subreddit)
            logger.info(f"Fetched {len(traffic)} days of traffic:")
            for record in traffic[:5]:  # Show first 5
                logger.info(
                    f"  {record['timestamp'].date()}: "
                    f"{record['unique_visitors']:,} visitors, "
                    f"{record['pageviews']:,} views, "
                    f"{record['subscriptions']:+,} subs"
                )
            if len(traffic) > 5:
                logger.info(f"  ... and {len(traffic) - 5} more days")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Dry run failed: {e}")
            sys.exit(1)

    # Run the sync
    success = sync_reddit_traffic(args.subreddit)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
