"""
Pulse - Multi-Platform Analytics Dashboard
Database Connection & Session Management
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import OperationalError, ProgrammingError
from contextlib import contextmanager

from database.models import Base
from services.logger import get_logger

logger = get_logger(__name__)


def get_database_url() -> str:
    """
    Get database URL from environment variable.
    Railway provides DATABASE_URL automatically for PostgreSQL addons.
    """
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set. "
            "Please configure your PostgreSQL connection string."
        )
    
    # Railway/Heroku use 'postgres://' but SQLAlchemy 1.4+ requires 'postgresql://'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    return database_url


def get_sql_echo() -> bool:
    """Check if SQL query logging is enabled via SQL_ECHO env var."""
    return os.getenv("SQL_ECHO", "false").lower() == "true"


# Create engine with connection pooling
engine = None
SessionLocal = None


def init_database():
    """
    Initialize database engine and create tables.
    Call this on application startup.
    """
    global engine, SessionLocal
    
    database_url = get_database_url()
    sql_echo = get_sql_echo()
    
    # Log connection (with masked password)
    safe_url = database_url.split("@")[-1] if "@" in database_url else "local"
    logger.info(f"Connecting to database: {safe_url}")
    
    if sql_echo:
        logger.warning("SQL_ECHO=True - All SQL queries will be logged (disable in production)")
    
    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,  # Verify connections before use
        echo=sql_echo,       # Print raw SQL queries when SQL_ECHO=True
        echo_pool=sql_echo   # Also log connection pool events
    )
    
    # Create all tables (safe - only creates if not exists)
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified/created successfully")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise
    
    # Create session factory
    SessionLocal = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    
    return engine


def get_session():
    """
    Get a database session.
    Use with context manager or ensure you close it manually.
    """
    if SessionLocal is None:
        init_database()
    return SessionLocal()


@contextmanager
def get_db_context():
    """
    Context manager for database sessions.
    Automatically handles commit/rollback and closing.
    
    Usage:
        with get_db_context() as db:
            db.query(Profile).all()
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database transaction failed: {e}")
        raise e
    finally:
        session.close()


# =============================================================================
# SCHEMA VALIDATION
# =============================================================================

def check_schema_health() -> dict:
    """
    Perform a health check on the database schema.
    Returns a dict with status and any missing columns.
    
    Usage:
        health = check_schema_health()
        if not health['healthy']:
            print(f"Missing columns: {health['missing_columns']}")
    """
    if engine is None:
        init_database()
    
    result = {
        "healthy": True,
        "missing_columns": [],
        "error": None
    }
    
    # Required columns for v0.0.2
    required_columns = {
        "profiles": ["platform", "platform_user_id", "user_role"],
        "posts": ["platform", "upvote_ratio", "is_crosspost", "retweet_count", "quote_count"]
    }
    
    try:
        with engine.connect() as conn:
            for table, columns in required_columns.items():
                for column in columns:
                    check = conn.execute(text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table AND column_name = :column
                    """), {"table": table, "column": column})
                    
                    if check.fetchone() is None:
                        result["missing_columns"].append(f"{table}.{column}")
                        result["healthy"] = False
        
        if result["missing_columns"]:
            logger.error(f"Schema health check FAILED - missing: {result['missing_columns']}")
        else:
            logger.info("Schema health check PASSED")
            
    except (OperationalError, ProgrammingError) as e:
        result["healthy"] = False
        result["error"] = str(e)
        logger.error(f"Schema health check ERROR: {e}")
    
    return result


def get_schema_version() -> str:
    """
    Attempt to detect current schema version based on columns present.
    """
    if engine is None:
        init_database()
    
    try:
        with engine.connect() as conn:
            # Check for v0.0.2 marker (platform column)
            check = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'profiles' AND column_name = 'platform'
            """))
            
            if check.fetchone():
                return "0.0.2"
            
            # Check for v0.0.1 (basic TikTok schema)
            check = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'profiles' AND column_name = 'tiktok_user_id'
            """))
            
            if check.fetchone():
                return "0.0.1"
            
            return "unknown"
            
    except Exception as e:
        logger.error(f"Failed to detect schema version: {e}")
        return "error"
