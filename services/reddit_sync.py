#!/usr/bin/env python3
"""
Pulse - Reddit Traffic Sync Service

Standalone script for fetching daily Reddit subreddit traffic statistics
by scraping the old.reddit.com traffic page. Designed for Railway cron execution.

Data is written to both:
1. PostgreSQL database (for dashboard display)
2. Excel spreadsheet (team_slack_addresses.xlsx) as source of truth

Usage:
    python -m services.reddit_sync

    # Or with specific subreddit:
    python -m services.reddit_sync --subreddit yoursubreddit

    # Spreadsheet only (skip database):
    python -m services.reddit_sync --spreadsheet-only

Environment Variables:
    REDDIT_SESSION_COOKIE - Reddit session cookie for authentication (reddit_session value)
    REDDIT_SUBREDDIT      - Default subreddit to track (without r/)
    DATABASE_URL          - PostgreSQL connection string
    SPREADSHEET_PATH      - Path to Excel file (default: team_slack_addresses.xlsx)

Railway Cron Schedule:
    0 0 * * * (runs daily at midnight UTC)
"""

import os
import sys
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.logger import get_logger, setup_root_logger

# Initialize logging
setup_root_logger()
logger = get_logger(__name__)

# Constants
DEFAULT_SPREADSHEET = "team_slack_addresses.xlsx"
TRAFFIC_SHEET_NAME = "Reddit_Traffic_History"
OLD_REDDIT_TRAFFIC_URL = "https://old.reddit.com/r/{subreddit}/about/traffic/"


def get_database_url() -> str:
    """Get database URL from environment."""
    database_url = os.getenv("DATABASE_URL", "")

    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    # Railway uses 'postgres://' but SQLAlchemy requires 'postgresql://'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    return database_url


def get_session_cookie() -> str:
    """Get Reddit session cookie from environment."""
    cookie = os.getenv("REDDIT_SESSION_COOKIE", "")

    if not cookie:
        raise ValueError(
            "REDDIT_SESSION_COOKIE environment variable is not set. "
            "Get this from your browser's cookies after logging into Reddit."
        )

    return cookie


def create_reddit_session() -> requests.Session:
    """
    Create a requests session with Reddit authentication cookies.

    Returns:
        Configured requests.Session with cookies set
    """
    session = requests.Session()

    # Get session cookie
    session_cookie = get_session_cookie()

    # Set cookies for old.reddit.com
    session.cookies.set("reddit_session", session_cookie, domain=".reddit.com")

    # Set headers to mimic a browser
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })

    return session


def parse_traffic_table(soup: BeautifulSoup, table_id: str) -> list[dict]:
    """
    Parse a traffic table from the Reddit traffic page.

    Args:
        soup: BeautifulSoup object of the page
        table_id: ID of the table to parse (e.g., 'traffic-by-day')

    Returns:
        List of dicts with traffic data
    """
    data = []

    # Find the table by ID or class
    table = soup.find("table", {"id": table_id})
    if not table:
        # Try finding by class
        table = soup.find("table", class_=lambda x: x and table_id in x)

    if not table:
        logger.warning(f"Could not find table with id/class: {table_id}")
        return data

    # Get all rows except header
    rows = table.find_all("tr")[1:]  # Skip header row

    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 3:
            try:
                # Parse date from first column
                date_text = cells[0].get_text(strip=True)
                # Handle various date formats Reddit uses
                timestamp = parse_reddit_date(date_text)

                # Parse numeric values (remove commas)
                uniques = parse_number(cells[1].get_text(strip=True))
                pageviews = parse_number(cells[2].get_text(strip=True))

                # Subscriptions may or may not be present
                subscriptions = 0
                if len(cells) >= 4:
                    subscriptions = parse_number(cells[3].get_text(strip=True))

                data.append({
                    "timestamp": timestamp,
                    "unique_visitors": uniques,
                    "pageviews": pageviews,
                    "subscriptions": subscriptions,
                })
            except (ValueError, IndexError) as e:
                logger.debug(f"Skipping row due to parse error: {e}")
                continue

    return data


def parse_reddit_date(date_str: str) -> datetime:
    """
    Parse Reddit's date format from the traffic page.

    Args:
        date_str: Date string like "Thursday, January 2, 2025" or "2025-01-02"

    Returns:
        datetime object
    """
    # Try various formats
    formats = [
        "%A, %B %d, %Y",  # Thursday, January 2, 2025
        "%B %d, %Y",       # January 2, 2025
        "%Y-%m-%d",        # 2025-01-02
        "%m/%d/%Y",        # 01/02/2025
        "%d %B %Y",        # 2 January 2025
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # Last resort: try to extract date components with regex
    match = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", date_str)
    if match:
        month, day, year = match.groups()
        return datetime(int(year), int(month), int(day))

    raise ValueError(f"Unable to parse date: {date_str}")


def parse_number(num_str: str) -> int:
    """
    Parse a number string, removing commas and handling edge cases.

    Args:
        num_str: Number string like "1,234" or "1234" or "-"

    Returns:
        Integer value (0 for invalid/empty)
    """
    if not num_str or num_str in ("-", "--", "N/A", ""):
        return 0

    # Remove commas, spaces, and other non-numeric chars except minus
    cleaned = re.sub(r"[^\d\-]", "", num_str)

    try:
        return int(cleaned) if cleaned else 0
    except ValueError:
        return 0


def scrape_subreddit_traffic(subreddit_name: str) -> list[dict]:
    """
    Scrape traffic statistics from old.reddit.com traffic page.

    Args:
        subreddit_name: Name of the subreddit (without r/)

    Returns:
        List of dicts with daily traffic data (timestamp, unique_visitors, pageviews, subscriptions)
    """
    url = OLD_REDDIT_TRAFFIC_URL.format(subreddit=subreddit_name)
    logger.info(f"Scraping traffic from: {url}")

    session = create_reddit_session()

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 403:
            raise PermissionError(
                f"Access denied to r/{subreddit_name} traffic page. "
                "Ensure your session cookie is valid and you have moderator access."
            )
        raise RuntimeError(f"HTTP error fetching traffic page: {e}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Network error fetching traffic page: {e}")

    # Check if we got redirected to login
    if "login" in response.url.lower() or "account/login" in response.text.lower():
        raise PermissionError(
            "Reddit session expired or invalid. Please update REDDIT_SESSION_COOKIE."
        )

    soup = BeautifulSoup(response.text, "html.parser")

    # Check for error messages
    error_div = soup.find("div", class_="error")
    if error_div:
        error_text = error_div.get_text(strip=True)
        raise PermissionError(f"Reddit error: {error_text}")

    # Parse the daily traffic table
    # Reddit's traffic page has tables with IDs like "traffic-by-day"
    # or tables within divs with specific classes
    daily_traffic = []

    # Try different table identifiers Reddit might use
    table_ids = ["traffic-by-day", "day-table", "daily"]

    for table_id in table_ids:
        daily_traffic = parse_traffic_table(soup, table_id)
        if daily_traffic:
            break

    # If no tables found by ID, try to find any table with traffic data
    if not daily_traffic:
        logger.info("Attempting to find traffic tables by structure...")
        tables = soup.find_all("table")

        for table in tables:
            # Look for tables that have date-like content in first column
            rows = table.find_all("tr")
            if len(rows) > 1:  # Has data rows
                first_data_row = rows[1].find_all("td")
                if first_data_row:
                    first_cell = first_data_row[0].get_text(strip=True)
                    # Check if it looks like a date
                    if any(month in first_cell for month in
                           ["January", "February", "March", "April", "May", "June",
                            "July", "August", "September", "October", "November", "December"]):
                        daily_traffic = parse_traffic_table_generic(table)
                        if daily_traffic:
                            break

    if not daily_traffic:
        logger.warning("No traffic data tables found on the page")
        # Log page structure for debugging
        logger.debug(f"Page title: {soup.title.string if soup.title else 'No title'}")
        logger.debug(f"Found {len(soup.find_all('table'))} tables on page")

    logger.info(f"Scraped {len(daily_traffic)} days of traffic data")
    return daily_traffic


def parse_traffic_table_generic(table) -> list[dict]:
    """
    Parse a generic traffic table by examining its structure.

    Args:
        table: BeautifulSoup table element

    Returns:
        List of traffic data dicts
    """
    data = []
    rows = table.find_all("tr")[1:]  # Skip header

    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 3:
            try:
                date_text = cells[0].get_text(strip=True)
                timestamp = parse_reddit_date(date_text)

                uniques = parse_number(cells[1].get_text(strip=True))
                pageviews = parse_number(cells[2].get_text(strip=True))
                subscriptions = parse_number(cells[3].get_text(strip=True)) if len(cells) >= 4 else 0

                data.append({
                    "timestamp": timestamp,
                    "unique_visitors": uniques,
                    "pageviews": pageviews,
                    "subscriptions": subscriptions,
                })
            except (ValueError, IndexError):
                continue

    return data


# =============================================================================
# SPREADSHEET INTEGRATION
# =============================================================================

def get_spreadsheet_path() -> Path:
    """Get the path to the tracking spreadsheet."""
    path_str = os.getenv("SPREADSHEET_PATH", DEFAULT_SPREADSHEET)
    return Path(path_str)


def write_to_spreadsheet(subreddit_name: str, traffic_data: list[dict]) -> int:
    """
    Write traffic data to Excel spreadsheet, avoiding duplicates.

    Args:
        subreddit_name: Name of the subreddit
        traffic_data: List of traffic records

    Returns:
        Number of new records written
    """
    spreadsheet_path = get_spreadsheet_path()
    logger.info(f"Writing to spreadsheet: {spreadsheet_path}")

    # Prepare data as DataFrame
    new_df = pd.DataFrame(traffic_data)
    new_df["subreddit"] = subreddit_name
    new_df["date"] = pd.to_datetime(new_df["timestamp"]).dt.date
    new_df["sync_timestamp"] = datetime.utcnow()

    # Reorder columns
    new_df = new_df[["date", "subreddit", "unique_visitors", "pageviews", "subscriptions", "sync_timestamp"]]

    try:
        # Check if file exists
        if spreadsheet_path.exists():
            # Read existing data
            try:
                existing_df = pd.read_excel(
                    spreadsheet_path,
                    sheet_name=TRAFFIC_SHEET_NAME,
                    engine="openpyxl"
                )
                existing_df["date"] = pd.to_datetime(existing_df["date"]).dt.date

                # Find new records (not in existing data)
                existing_dates = set(
                    (row["date"], row["subreddit"])
                    for _, row in existing_df.iterrows()
                )

                new_records = new_df[
                    ~new_df.apply(
                        lambda row: (row["date"], row["subreddit"]) in existing_dates,
                        axis=1
                    )
                ]

                if new_records.empty:
                    logger.info("No new records to add to spreadsheet")
                    return 0

                # Append new records
                combined_df = pd.concat([existing_df, new_records], ignore_index=True)
                combined_df = combined_df.sort_values(["subreddit", "date"], ascending=[True, False])

            except ValueError:
                # Sheet doesn't exist, create new
                logger.info(f"Creating new sheet: {TRAFFIC_SHEET_NAME}")
                combined_df = new_df
                new_records = new_df

            # Write back to spreadsheet (preserving other sheets)
            with pd.ExcelWriter(
                spreadsheet_path,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace"
            ) as writer:
                combined_df.to_excel(writer, sheet_name=TRAFFIC_SHEET_NAME, index=False)

        else:
            # Create new spreadsheet
            logger.info(f"Creating new spreadsheet: {spreadsheet_path}")
            new_df.to_excel(spreadsheet_path, sheet_name=TRAFFIC_SHEET_NAME, index=False, engine="openpyxl")
            new_records = new_df

        records_written = len(new_records)
        logger.info(f"Wrote {records_written} new records to spreadsheet")
        return records_written

    except Exception as e:
        logger.error(f"Spreadsheet error: {e}")
        raise


def read_from_spreadsheet(subreddit_name: Optional[str] = None) -> pd.DataFrame:
    """
    Read traffic data from spreadsheet.

    Args:
        subreddit_name: Filter by subreddit (None for all)

    Returns:
        DataFrame with traffic data
    """
    spreadsheet_path = get_spreadsheet_path()

    if not spreadsheet_path.exists():
        logger.warning(f"Spreadsheet not found: {spreadsheet_path}")
        return pd.DataFrame()

    try:
        df = pd.read_excel(spreadsheet_path, sheet_name=TRAFFIC_SHEET_NAME, engine="openpyxl")

        if subreddit_name:
            df = df[df["subreddit"] == subreddit_name]

        return df
    except ValueError:
        logger.warning(f"Sheet '{TRAFFIC_SHEET_NAME}' not found in spreadsheet")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading spreadsheet: {e}")
        return pd.DataFrame()


# =============================================================================
# DATABASE INTEGRATION (kept for dashboard compatibility)
# =============================================================================

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
        logger.info(f"Upserted {upserted_count} traffic records to database")
        return upserted_count

    except Exception as e:
        session.rollback()
        logger.error(f"Database error during upsert: {e}")
        raise
    finally:
        session.close()


# =============================================================================
# MAIN SYNC FUNCTION
# =============================================================================

def sync_reddit_traffic(
    subreddit_name: Optional[str] = None,
    spreadsheet_only: bool = False,
    database_only: bool = False
) -> bool:
    """
    Main sync function - scrapes and stores Reddit traffic data.

    Args:
        subreddit_name: Subreddit to sync (uses REDDIT_SUBREDDIT env var if not provided)
        spreadsheet_only: Only write to spreadsheet, skip database
        database_only: Only write to database, skip spreadsheet

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
    subreddit_name = subreddit_name.replace("r/", "").strip()

    logger.info("=" * 60)
    logger.info(f"REDDIT TRAFFIC SYNC - r/{subreddit_name}")
    logger.info(f"Started at: {datetime.utcnow().isoformat()}Z")
    logger.info(f"Mode: {'Spreadsheet Only' if spreadsheet_only else 'Database Only' if database_only else 'Full Sync'}")
    logger.info("=" * 60)

    try:
        # Scrape traffic from old.reddit.com
        traffic_data = scrape_subreddit_traffic(subreddit_name)

        if not traffic_data:
            logger.warning(f"No traffic data scraped for r/{subreddit_name}")
            return True  # Not an error, just no data

        spreadsheet_records = 0
        database_records = 0

        # Write to spreadsheet (source of truth)
        if not database_only:
            try:
                spreadsheet_records = write_to_spreadsheet(subreddit_name, traffic_data)
            except Exception as e:
                logger.error(f"Spreadsheet write failed: {e}")
                if spreadsheet_only:
                    return False

        # Write to database (for dashboard)
        if not spreadsheet_only:
            try:
                database_records = upsert_traffic_data(subreddit_name, traffic_data)
            except Exception as e:
                logger.error(f"Database write failed: {e}")
                # Continue even if database fails - spreadsheet is source of truth

        logger.info("=" * 60)
        logger.info("SYNC COMPLETE")
        logger.info(f"  Subreddit: r/{subreddit_name}")
        logger.info(f"  Records scraped: {len(traffic_data)}")
        if not database_only:
            logger.info(f"  Spreadsheet (new): {spreadsheet_records}")
        if not spreadsheet_only:
            logger.info(f"  Database (upserted): {database_records}")
        logger.info(f"  Completed at: {datetime.utcnow().isoformat()}Z")
        logger.info("=" * 60)

        return True

    except PermissionError as e:
        logger.error(f"Permission denied: {e}")
        return False
    except RuntimeError as e:
        logger.error(f"Runtime error: {e}")
        return False
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sync Reddit subreddit traffic statistics via web scraping"
    )
    parser.add_argument(
        "--subreddit", "-s",
        type=str,
        help="Subreddit name (without r/). Defaults to REDDIT_SUBREDDIT env var",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape data but don't store anywhere",
    )
    parser.add_argument(
        "--spreadsheet-only",
        action="store_true",
        help="Only write to spreadsheet, skip database",
    )
    parser.add_argument(
        "--database-only",
        action="store_true",
        help="Only write to database, skip spreadsheet",
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY RUN MODE - Data will be scraped but not stored")
        subreddit = args.subreddit or os.getenv("REDDIT_SUBREDDIT", "").replace("r/", "")

        if not subreddit:
            logger.error("No subreddit specified")
            sys.exit(1)

        try:
            traffic = scrape_subreddit_traffic(subreddit)
            logger.info(f"Scraped {len(traffic)} days of traffic:")
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
    success = sync_reddit_traffic(
        args.subreddit,
        spreadsheet_only=args.spreadsheet_only,
        database_only=args.database_only
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
