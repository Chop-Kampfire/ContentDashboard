"""
Pulse - TikTok Analytics Dashboard
Telegram Bot Integration for Viral Alerts
"""

import httpx
import logging
from typing import Optional
from datetime import datetime

from config import config

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """
    Sends notifications via Telegram Bot API.
    
    Usage:
        notifier = TelegramNotifier()
        await notifier.send_viral_alert(username="creator", views=1000000, avg_views=100000)
    """
    
    BASE_URL = "https://api.telegram.org/bot{token}"
    
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        self.bot_token = bot_token or config.TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or config.TELEGRAM_CHAT_ID
        self.api_url = self.BASE_URL.format(token=self.bot_token)
        
    async def send_message(self, text: str, parse_mode: str = "HTML") -> dict:
        """
        Send a message to the configured Telegram chat.
        
        Args:
            text: Message content (supports HTML formatting)
            parse_mode: 'HTML' or 'Markdown'
            
        Returns:
            Telegram API response dict
        """
        endpoint = f"{self.api_url}/sendMessage"
        
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": False
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(endpoint, json=payload, timeout=30.0)
                response.raise_for_status()
                result = response.json()
                
                if result.get("ok"):
                    logger.info(f"✅ Telegram message sent successfully")
                else:
                    logger.error(f"❌ Telegram API error: {result}")
                    
                return result
                
            except httpx.HTTPError as e:
                logger.error(f"❌ Failed to send Telegram message: {e}")
                return {"ok": False, "error": str(e)}
    
    async def send_viral_alert(
        self,
        username: str,
        post_id: str,
        views: int,
        avg_views: float,
        description: Optional[str] = None,
        video_url: Optional[str] = None
    ) -> dict:
        """
        Send a viral post alert with rich formatting.
        
        Args:
            username: TikTok handle (without @)
            post_id: TikTok post ID
            views: Current view count
            avg_views: Account's average post views
            description: Post caption (truncated)
            video_url: Direct link to post
            
        Returns:
            Telegram API response
        """
        # Calculate performance multiplier
        if avg_views > 0:
            performance_pct = ((views - avg_views) / avg_views) * 100
            multiplier = views / avg_views
        else:
            performance_pct = 0
            multiplier = 0
        
        # Format view counts
        def format_number(n: int) -> str:
            if n >= 1_000_000:
                return f"{n/1_000_000:.1f}M"
            elif n >= 1_000:
                return f"{n/1_000:.1f}K"
            return str(n)
        
        # Truncate description
        desc_preview = ""
        if description:
            desc_preview = description[:100] + "..." if len(description) > 100 else description
            desc_preview = f"\n\n📝 <i>{desc_preview}</i>"
        
        # Build the message
        message = f"""🚀 <b>VIRAL ALERT!</b>

<b>@{username}</b> just posted a video that is performing <b>{performance_pct:,.0f}%</b> better than their average!

📊 <b>Stats:</b>
├ Views: <code>{format_number(views)}</code>
├ Avg Views: <code>{format_number(int(avg_views))}</code>
└ Multiplier: <b>{multiplier:.1f}x</b>{desc_preview}

🔗 <a href="https://www.tiktok.com/@{username}/video/{post_id}">View Post</a>

⏰ Detected: {datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M UTC')}"""
        
        return await self.send_message(message)
    
    async def send_welcome_alert(self, username: str, follower_count: int) -> dict:
        """Send notification when a new profile is added to watchlist."""
        
        def format_number(n: int) -> str:
            if n >= 1_000_000:
                return f"{n/1_000_000:.1f}M"
            elif n >= 1_000:
                return f"{n/1_000:.1f}K"
            return str(n)
        
        message = f"""✅ <b>New Profile Added to Watchlist</b>

👤 <b>@{username}</b>
👥 Followers: <code>{format_number(follower_count)}</code>

Pulse will now monitor this account for viral posts.
Updates every {config.SCRAPE_INTERVAL_HOURS} hours."""
        
        return await self.send_message(message)
    
    async def send_error_alert(self, error_type: str, details: str) -> dict:
        """Send error notification for monitoring."""
        
        message = f"""⚠️ <b>Pulse Error Alert</b>

Type: <code>{error_type}</code>
Details: {details}

⏰ {datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M UTC')}"""
        
        return await self.send_message(message)


# Synchronous wrapper for non-async contexts
def send_viral_alert_sync(
    username: str,
    post_id: str,
    views: int,
    avg_views: float,
    description: Optional[str] = None
) -> dict:
    """Synchronous wrapper for sending viral alerts."""
    import asyncio
    
    notifier = TelegramNotifier()
    
    # Handle running in existing event loop (e.g., Streamlit)
    try:
        loop = asyncio.get_running_loop()
        # We're in an async context, create a task
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                asyncio.run,
                notifier.send_viral_alert(username, post_id, views, avg_views, description)
            )
            return future.result(timeout=30)
    except RuntimeError:
        # No running loop, safe to use asyncio.run
        return asyncio.run(
            notifier.send_viral_alert(username, post_id, views, avg_views, description)
        )

