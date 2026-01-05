"""
Pulse Database Migration Script - v0.0.2
Standalone script to add multi-platform support columns.

Usage:
    python migrate_v002.py

Requires DATABASE_URL environment variable to be set.
"""

import os
import sys

def get_database_url():
    """Get database URL from environment."""
    database_url = os.getenv("DATABASE_URL", "")
    
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)
    
    # Railway uses 'postgres://' but SQLAlchemy requires 'postgresql://'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    return database_url


def column_exists(connection, table_name, column_name):
    """Check if a column exists in a table."""
    from sqlalchemy import text
    
    result = connection.execute(text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = :table AND column_name = :column
    """), {"table": table_name, "column": column_name})
    
    return result.fetchone() is not None


def add_column_if_not_exists(connection, table_name, column_name, column_type, default=None):
    """Add a column to a table if it doesn't exist."""
    from sqlalchemy import text
    
    if column_exists(connection, table_name, column_name):
        print(f"  ✓ {table_name}.{column_name} already exists")
        return False
    
    # Build the ALTER TABLE statement
    sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
    if default is not None:
        sql += f" DEFAULT {default}"
    
    connection.execute(text(sql))
    print(f"  + Added {table_name}.{column_name} ({column_type})")
    return True


def run_migration():
    """Run the v0.0.2 migration."""
    
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        print("ERROR: SQLAlchemy not installed. Run: pip install sqlalchemy psycopg2-binary")
        sys.exit(1)
    
    database_url = get_database_url()
    
    print("=" * 60)
    print("Pulse Database Migration v0.0.2 - Multi-Platform Support")
    print("=" * 60)
    print()
    
    # Mask password in URL for display
    display_url = database_url.split("@")[-1] if "@" in database_url else "local"
    print(f"Database: {display_url}")
    print()
    
    engine = create_engine(database_url)
    
    with engine.connect() as connection:
        # Start transaction
        trans = connection.begin()
        
        try:
            # =========================================================
            # PROFILES TABLE
            # =========================================================
            print("[1/2] Updating 'profiles' table...")
            
            # Platform column (defaults to 'tiktok' for existing records)
            add_column_if_not_exists(
                connection, "profiles", "platform", 
                "VARCHAR(32)", "'tiktok'"
            )
            
            # Platform user ID (renamed from tiktok_user_id conceptually)
            add_column_if_not_exists(
                connection, "profiles", "platform_user_id", 
                "VARCHAR(64)", "NULL"
            )
            
            # User role for analytics
            add_column_if_not_exists(
                connection, "profiles", "user_role", 
                "VARCHAR(32)", "'creator'"
            )
            
            # Reddit-specific columns
            add_column_if_not_exists(
                connection, "profiles", "subreddit_name", 
                "VARCHAR(128)", "NULL"
            )
            
            add_column_if_not_exists(
                connection, "profiles", "subreddit_subscribers", 
                "BIGINT", "NULL"
            )
            
            add_column_if_not_exists(
                connection, "profiles", "active_users", 
                "INTEGER", "NULL"
            )
            
            print()
            
            # =========================================================
            # POSTS TABLE
            # =========================================================
            print("[2/2] Updating 'posts' table...")
            
            # Platform column (defaults to 'tiktok' for existing records)
            add_column_if_not_exists(
                connection, "posts", "platform", 
                "VARCHAR(32)", "'tiktok'"
            )
            
            # Reddit-specific columns
            add_column_if_not_exists(
                connection, "posts", "upvote_ratio", 
                "FLOAT", "NULL"
            )
            
            add_column_if_not_exists(
                connection, "posts", "is_crosspost", 
                "BOOLEAN", "NULL"
            )
            
            add_column_if_not_exists(
                connection, "posts", "original_subreddit", 
                "VARCHAR(128)", "NULL"
            )
            
            # Twitter-specific columns
            add_column_if_not_exists(
                connection, "posts", "retweet_count", 
                "BIGINT", "NULL"
            )
            
            add_column_if_not_exists(
                connection, "posts", "quote_count", 
                "BIGINT", "NULL"
            )
            
            # Commit the transaction
            trans.commit()
            
            print()
            print("=" * 60)
            print("✅ Migration v0.0.2 completed successfully!")
            print("   All existing data preserved with platform='tiktok'")
            print("=" * 60)
            
        except Exception as e:
            trans.rollback()
            print()
            print(f"❌ Migration failed: {e}")
            print("   Transaction rolled back - no changes made.")
            raise


if __name__ == "__main__":
    run_migration()

