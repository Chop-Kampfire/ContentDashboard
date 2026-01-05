"""
Pulse - TikTok Analytics Dashboard
Database Connection & Session Management
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager

from database.models import Base


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
    
    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,  # Verify connections before use
        echo=os.getenv("SQL_DEBUG", "false").lower() == "true"
    )
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
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
        raise e
    finally:
        session.close()

