"""
Pulse - Multi-Platform Analytics Dashboard
Centralized Logging Configuration

Usage:
    from services.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Starting scrape for @username")
"""

import os
import sys
import logging
from typing import Optional


# =============================================================================
# LOG FORMAT FOR RAILWAY
# =============================================================================
# Railway streams stdout/stderr - use a format that's searchable and parseable
# Format: [LEVEL] [module_name] message

RAILWAY_FORMAT = "[%(levelname)s] [%(name)s] %(message)s"
DETAILED_FORMAT = "%(asctime)s | [%(levelname)s] [%(name)s:%(lineno)d] %(message)s"


def get_log_level() -> int:
    """Get log level from environment variable."""
    level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    
    return levels.get(level_str, logging.INFO)


def is_debug_mode() -> bool:
    """Check if running in debug/development mode."""
    return os.getenv("DEBUG", "false").lower() == "true"


def setup_root_logger():
    """
    Configure the root logger for the application.
    Called once at application startup.
    """
    log_level = get_log_level()
    use_detailed = is_debug_mode()
    
    # Choose format based on environment
    log_format = DETAILED_FORMAT if use_detailed else RAILWAY_FORMAT
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)  # Railway captures stdout
        ],
        force=True  # Override any existing configuration
    )
    
    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.DEBUG if os.getenv("SQL_ECHO", "false").lower() == "true" 
        else logging.WARNING
    )
    
    root_logger = logging.getLogger()
    root_logger.info(f"Logging initialized | level={logging.getLevelName(log_level)} | debug={use_detailed}")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance for a module.
    
    Args:
        name: Module name (typically __name__). If None, returns root logger.
    
    Returns:
        Configured logger instance.
    
    Example:
        logger = get_logger(__name__)
        logger.info("Processing profile", extra={"username": "example"})
    """
    return logging.getLogger(name)


# =============================================================================
# STRUCTURED LOGGING HELPERS
# =============================================================================

def log_api_call(logger: logging.Logger, method: str, url: str, status: int, duration_ms: float):
    """Log an API call with consistent format."""
    logger.info(
        f"API {method} {url} | status={status} | duration={duration_ms:.0f}ms"
    )


def log_db_operation(logger: logging.Logger, operation: str, table: str, count: int = 1):
    """Log a database operation with consistent format."""
    logger.debug(f"DB {operation} | table={table} | rows={count}")


def log_scrape_result(logger: logging.Logger, username: str, platform: str, 
                      posts_count: int, success: bool, error: Optional[str] = None):
    """Log a scrape operation result."""
    if success:
        logger.info(f"SCRAPE OK | platform={platform} | user=@{username} | posts={posts_count}")
    else:
        logger.error(f"SCRAPE FAIL | platform={platform} | user=@{username} | error={error}")


def log_viral_alert(logger: logging.Logger, username: str, platform: str, 
                    post_id: str, views: int, threshold: int):
    """Log when a viral post is detected."""
    logger.warning(
        f"VIRAL DETECTED | platform={platform} | user=@{username} | "
        f"post={post_id} | views={views:,} | threshold={threshold:,}"
    )


# =============================================================================
# INITIALIZATION
# =============================================================================

# Auto-setup on import (safe to call multiple times due to force=True)
setup_root_logger()

