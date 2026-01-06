"""
Pulse - Multi-Platform Analytics Dashboard
Background Worker

Runs a scheduled job every 6 hours to update all tracked profiles.
Deploy this as a separate process on Railway.

Usage:
    python worker.py
    
Environment Variables:
    DATABASE_URL - PostgreSQL connection string
    RAPIDAPI_KEY - TikTok API key
    TELEGRAM_BOT_TOKEN - Telegram bot token
    TELEGRAM_CHAT_ID - Telegram chat ID for alerts
    SCRAPE_INTERVAL_HOURS - Update interval (default: 6)
    LOG_LEVEL - Logging level (default: INFO)
    SQL_ECHO - Print SQL queries (default: false)
"""

import asyncio
from datetime import datetime
import signal
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import config
from database.connection import init_database, check_schema_health
from scraper import update_all_profiles
from services.telegram_notifier import TelegramNotifier
from services.logger import get_logger, setup_root_logger

# Initialize logging
setup_root_logger()
logger = get_logger(__name__)


class PulseWorker:
    """
    Background worker that periodically updates TikTok data.
    """
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.interval_hours = config.SCRAPE_INTERVAL_HOURS
        self.telegram = TelegramNotifier()
        self.is_running = False
        
    async def startup(self):
        """Initialize database and start scheduler."""
        logger.info("Pulse Worker starting up...")
        
        # Validate config
        missing = config.validate()
        if missing:
            logger.error(f"Missing required environment variables: {missing}")
            sys.exit(1)
        
        # Initialize database
        try:
            init_database()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            sys.exit(1)
        
        # Check schema health
        schema_health = check_schema_health()
        if not schema_health["healthy"]:
            logger.error("=" * 70)
            logger.error("DATABASE SCHEMA HEALTH CHECK FAILED")
            logger.error("=" * 70)
            logger.error(f"Current schema version: {schema_health.get('schema_version', 'unknown')}")
            logger.error(f"Missing {len(schema_health.get('missing_columns', []))} columns:")
            for col in schema_health.get('missing_columns', []):
                logger.error(f"  - {col}")
            logger.error("")
            logger.error("The migration should have run automatically during deployment.")
            logger.error("If you're seeing this, the migration may have failed.")
            logger.error("")
            logger.error("To fix manually:")
            logger.error("  1. Run: python -m database.migrations.run_all_migrations")
            logger.error("  2. Or redeploy to trigger automatic migration")
            logger.error("=" * 70)

            # Send alert to Telegram
            try:
                await self.telegram.send_message(
                    f"🔴 <b>Pulse Worker Failed to Start</b>\n\n"
                    f"<b>Reason:</b> Database schema health check failed\n"
                    f"<b>Schema Version:</b> {schema_health.get('schema_version', 'unknown')}\n"
                    f"<b>Missing Columns:</b> {len(schema_health.get('missing_columns', []))}\n\n"
                    f"<b>Action Required:</b>\n"
                    f"Run: <code>python -m database.migrations.run_all_migrations</code>\n"
                    f"Or redeploy on Railway to trigger automatic migration."
                )
            except:
                pass

            sys.exit(1)

        logger.info(f"✅ Schema health check PASSED - version {schema_health.get('schema_version', 'unknown')}")
        
        # Schedule the job
        self.scheduler.add_job(
            self.run_update_job,
            trigger=IntervalTrigger(hours=self.interval_hours),
            id="profile_update",
            name="Update All Profiles",
            replace_existing=True,
            next_run_time=datetime.utcnow()  # Run immediately on startup
        )
        
        self.scheduler.start()
        self.is_running = True
        
        logger.info(
            f"Scheduler started | interval={self.interval_hours}h | next_run=NOW"
        )
        
        # Send startup notification
        try:
            await self.telegram.send_message(
                f"🟢 <b>Pulse Worker Started</b>\n\n"
                f"Update interval: Every {self.interval_hours} hours\n"
                f"Schema version: {schema_health.get('schema_version', 'unknown')}\n"
                f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            )
        except Exception as e:
            logger.warning(f"Could not send startup notification: {e}")
    
    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down Pulse Worker...")
        
        self.scheduler.shutdown(wait=False)
        self.is_running = False
        
        try:
            await self.telegram.send_message(
                f"🔴 <b>Pulse Worker Stopped</b>\n\n"
                f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            )
        except:
            pass
        
        logger.info("Pulse Worker stopped")
    
    async def run_update_job(self):
        """Execute the profile update job."""
        start_time = datetime.utcnow()
        logger.info(f"Starting scheduled update | time={start_time.isoformat()}")
        
        try:
            results = await update_all_profiles()
            
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info(
                f"Update complete | duration={elapsed:.1f}s | "
                f"success={results['success']} | failed={results['failed']}"
            )
            
            # Send summary if there were failures
            if results['failed'] > 0:
                await self.telegram.send_message(
                    f"⚠️ <b>Update Complete with Errors</b>\n\n"
                    f"✅ Success: {results['success']}\n"
                    f"❌ Failed: {results['failed']}\n"
                    f"⏱ Duration: {elapsed:.1f}s"
                )
                
        except Exception as e:
            logger.error(f"Update job failed: {e}", exc_info=True)
            
            try:
                await self.telegram.send_error_alert(
                    error_type="SCHEDULED_UPDATE_FAILED",
                    details=str(e)
                )
            except:
                pass
    
    async def run_forever(self):
        """Keep the worker running."""
        await self.startup()
        
        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.shutdown())
                )
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                pass
        
        # Keep running until shutdown
        while self.is_running:
            await asyncio.sleep(1)


async def main():
    """Entry point for the worker."""
    logger.info("=" * 60)
    logger.info("PULSE WORKER INITIALIZING")
    logger.info("=" * 60)
    
    worker = PulseWorker()
    
    try:
        await worker.run_forever()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        await worker.shutdown()
    except Exception as e:
        logger.error(f"Worker crashed: {e}", exc_info=True)
        await worker.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
