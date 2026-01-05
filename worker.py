"""
Pulse - TikTok Analytics Dashboard
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
"""

import asyncio
import logging
from datetime import datetime
import signal
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import config
from database import init_database
from scraper import update_all_profiles
from services.telegram_notifier import TelegramNotifier

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)


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
        logger.info("🚀 Pulse Worker starting up...")
        
        # Validate config
        missing = config.validate()
        if missing:
            logger.error(f"Missing required environment variables: {missing}")
            sys.exit(1)
        
        # Initialize database
        try:
            init_database()
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            sys.exit(1)
        
        # Schedule the job
        self.scheduler.add_job(
            self.run_update_job,
            trigger=IntervalTrigger(hours=self.interval_hours),
            id="tiktok_update",
            name="Update TikTok Profiles",
            replace_existing=True,
            next_run_time=datetime.utcnow()  # Run immediately on startup
        )
        
        self.scheduler.start()
        self.is_running = True
        
        logger.info(
            f"⏰ Scheduler started. Updates every {self.interval_hours} hours. "
            f"Next run: NOW"
        )
        
        # Send startup notification
        try:
            await self.telegram.send_message(
                f"🟢 <b>Pulse Worker Started</b>\n\n"
                f"Update interval: Every {self.interval_hours} hours\n"
                f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            )
        except Exception as e:
            logger.warning(f"Could not send startup notification: {e}")
    
    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("🛑 Shutting down Pulse Worker...")
        
        self.scheduler.shutdown(wait=False)
        self.is_running = False
        
        try:
            await self.telegram.send_message(
                f"🔴 <b>Pulse Worker Stopped</b>\n\n"
                f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            )
        except:
            pass
        
        logger.info("👋 Pulse Worker stopped")
    
    async def run_update_job(self):
        """Execute the profile update job."""
        start_time = datetime.utcnow()
        logger.info(f"🔄 Starting scheduled update at {start_time}")
        
        try:
            results = await update_all_profiles()
            
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info(
                f"✅ Update complete in {elapsed:.1f}s | "
                f"Success: {results['success']} | "
                f"Failed: {results['failed']}"
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
            logger.error(f"❌ Update job failed: {e}", exc_info=True)
            
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
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self.shutdown())
            )
        
        # Keep running until shutdown
        while self.is_running:
            await asyncio.sleep(1)


async def main():
    """Entry point for the worker."""
    worker = PulseWorker()
    
    try:
        await worker.run_forever()
    except KeyboardInterrupt:
        await worker.shutdown()
    except Exception as e:
        logger.error(f"Worker crashed: {e}", exc_info=True)
        await worker.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

