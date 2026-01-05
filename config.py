"""
Pulse - TikTok Analytics Dashboard
Configuration & Environment Variables
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Application configuration loaded from environment variables."""
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    
    # RapidAPI TikTok
    RAPIDAPI_KEY: str = os.getenv("RAPIDAPI_KEY", "")
    RAPIDAPI_HOST: str = os.getenv("RAPIDAPI_HOST", "tiktok-all-in-one.p.rapidapi.com")
    
    # Telegram Bot
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")  # Your chat/channel ID
    
    # Scraper Settings
    VIRAL_THRESHOLD_MULTIPLIER: float = float(os.getenv("VIRAL_THRESHOLD", "5.0"))
    POSTS_LOOKBACK_DAYS: int = int(os.getenv("POSTS_LOOKBACK_DAYS", "30"))
    SCRAPE_INTERVAL_HOURS: int = int(os.getenv("SCRAPE_INTERVAL_HOURS", "6"))
    
    # Debug
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    def validate(self) -> list[str]:
        """Validate required configuration. Returns list of missing vars."""
        missing = []
        
        if not self.DATABASE_URL:
            missing.append("DATABASE_URL")
        if not self.RAPIDAPI_KEY:
            missing.append("RAPIDAPI_KEY")
        if not self.TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not self.TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID")
            
        return missing


# Global config instance
config = Config()

