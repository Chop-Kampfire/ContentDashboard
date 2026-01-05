"""
Pulse Database Migration - v0.0.2 Multi-Platform Support

This migration adds support for Twitter/X and Reddit platforms while
preserving all existing TikTok data.

Changes:
- Creates platform_type and user_role_type enums
- Adds platform column to profiles and posts tables
- Renames tiktok_user_id to platform_user_id
- Renames tiktok_post_id to platform_post_id  
- Adds Reddit-specific columns (subreddit_subscribers, active_users, upvote_ratio, etc.)
- Adds Twitter-specific columns (retweet_count, quote_count, bookmark_count, etc.)
- Updates unique constraints to be platform-aware
- Updates views for multi-platform support

Usage:
    python -m database.migrations.v002_multiplatform

Environment Variables:
    DATABASE_URL - PostgreSQL connection string
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


def get_database_url() -> str:
    """Get database URL from environment."""
    database_url = os.getenv("DATABASE_URL", "")
    
    if not database_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set. "
            "Please set your PostgreSQL connection string."
        )
    
    # Railway/Heroku use 'postgres://' but SQLAlchemy requires 'postgresql://'
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    return database_url


def run_migration():
    """Execute the database migration."""
    
    try:
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker
    except ImportError:
        logger.error("SQLAlchemy not installed. Run: pip install sqlalchemy psycopg2-binary")
        sys.exit(1)
    
    database_url = get_database_url()
    logger.info("🔄 Starting migration v0.0.2 - Multi-Platform Support")
    logger.info(f"📊 Database: {database_url.split('@')[-1] if '@' in database_url else 'local'}")
    
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Check current state
        logger.info("📋 Checking current database state...")
        
        result = session.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'profiles' AND column_name = 'platform'
        """))
        
        if result.fetchone():
            logger.info("✅ Migration already applied (platform column exists)")
            return True
        
        logger.info("🚀 Applying migration...")
        
        # =================================================================
        # STEP 1: Create ENUM types
        # =================================================================
        logger.info("  [1/8] Creating enum types...")
        
        session.execute(text("""
            DO $$ BEGIN
                CREATE TYPE platform_type AS ENUM ('tiktok', 'twitter', 'reddit');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        
        session.execute(text("""
            DO $$ BEGIN
                CREATE TYPE user_role_type AS ENUM ('creator', 'moderator', 'power_user', 'brand');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        
        session.commit()
        
        # =================================================================
        # STEP 2: Add platform columns to profiles table
        # =================================================================
        logger.info("  [2/8] Adding columns to profiles table...")
        
        # Add platform column
        session.execute(text("""
            ALTER TABLE profiles 
            ADD COLUMN IF NOT EXISTS platform platform_type DEFAULT 'tiktok'
        """))
        
        # Add user_role column
        session.execute(text("""
            ALTER TABLE profiles 
            ADD COLUMN IF NOT EXISTS user_role user_role_type DEFAULT 'creator'
        """))
        
        # Add Reddit-specific columns
        session.execute(text("""
            ALTER TABLE profiles 
            ADD COLUMN IF NOT EXISTS subreddit_name VARCHAR(128)
        """))
        
        session.execute(text("""
            ALTER TABLE profiles 
            ADD COLUMN IF NOT EXISTS subreddit_subscribers BIGINT
        """))
        
        session.execute(text("""
            ALTER TABLE profiles 
            ADD COLUMN IF NOT EXISTS active_users INTEGER
        """))
        
        session.commit()
        
        # =================================================================
        # STEP 3: Rename tiktok_user_id to platform_user_id
        # =================================================================
        logger.info("  [3/8] Renaming tiktok_user_id to platform_user_id...")
        
        # Check if old column exists
        result = session.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'profiles' AND column_name = 'tiktok_user_id'
        """))
        
        if result.fetchone():
            session.execute(text("""
                ALTER TABLE profiles 
                RENAME COLUMN tiktok_user_id TO platform_user_id
            """))
            session.commit()
        else:
            # Column might already be renamed or doesn't exist
            session.execute(text("""
                ALTER TABLE profiles 
                ADD COLUMN IF NOT EXISTS platform_user_id VARCHAR(64)
            """))
            session.commit()
        
        # =================================================================
        # STEP 4: Add columns to posts table
        # =================================================================
        logger.info("  [4/8] Adding columns to posts table...")
        
        # Add platform column
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS platform platform_type DEFAULT 'tiktok'
        """))
        
        # Twitter-specific columns
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS retweet_count BIGINT
        """))
        
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS quote_count BIGINT
        """))
        
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS bookmark_count BIGINT
        """))
        
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS impression_count BIGINT
        """))
        
        # Reddit-specific columns
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS upvote_ratio FLOAT
        """))
        
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS is_crosspost BOOLEAN
        """))
        
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS original_subreddit VARCHAR(128)
        """))
        
        session.execute(text("""
            ALTER TABLE posts 
            ADD COLUMN IF NOT EXISTS reddit_score INTEGER
        """))
        
        session.commit()
        
        # =================================================================
        # STEP 5: Rename tiktok_post_id to platform_post_id
        # =================================================================
        logger.info("  [5/8] Renaming tiktok_post_id to platform_post_id...")
        
        result = session.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'posts' AND column_name = 'tiktok_post_id'
        """))
        
        if result.fetchone():
            session.execute(text("""
                ALTER TABLE posts 
                RENAME COLUMN tiktok_post_id TO platform_post_id
            """))
            session.commit()
        else:
            session.execute(text("""
                ALTER TABLE posts 
                ADD COLUMN IF NOT EXISTS platform_post_id VARCHAR(64)
            """))
            session.commit()
        
        # =================================================================
        # STEP 6: Add columns to profile_history table
        # =================================================================
        logger.info("  [6/8] Adding columns to profile_history table...")
        
        session.execute(text("""
            ALTER TABLE profile_history 
            ADD COLUMN IF NOT EXISTS subreddit_subscribers BIGINT
        """))
        
        session.execute(text("""
            ALTER TABLE profile_history 
            ADD COLUMN IF NOT EXISTS active_users INTEGER
        """))
        
        session.commit()
        
        # =================================================================
        # STEP 7: Add columns to post_history table
        # =================================================================
        logger.info("  [7/8] Adding columns to post_history table...")
        
        session.execute(text("""
            ALTER TABLE post_history 
            ADD COLUMN IF NOT EXISTS retweet_count BIGINT
        """))
        
        session.execute(text("""
            ALTER TABLE post_history 
            ADD COLUMN IF NOT EXISTS quote_count BIGINT
        """))
        
        session.execute(text("""
            ALTER TABLE post_history 
            ADD COLUMN IF NOT EXISTS upvote_ratio FLOAT
        """))
        
        session.execute(text("""
            ALTER TABLE post_history 
            ADD COLUMN IF NOT EXISTS reddit_score INTEGER
        """))
        
        session.commit()
        
        # =================================================================
        # STEP 8: Add platform column to alert_logs
        # =================================================================
        logger.info("  [8/8] Adding platform column to alert_logs...")
        
        session.execute(text("""
            ALTER TABLE alert_logs 
            ADD COLUMN IF NOT EXISTS platform platform_type
        """))
        
        session.commit()
        
        # =================================================================
        # STEP 9: Update constraints and indexes
        # =================================================================
        logger.info("  [9/9] Updating constraints and indexes...")
        
        # Drop old unique constraint on username if it exists
        session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE profiles DROP CONSTRAINT IF EXISTS profiles_username_key;
            EXCEPTION
                WHEN undefined_object THEN null;
            END $$;
        """))
        
        # Drop old unique constraint on tiktok_user_id if it exists
        session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE profiles DROP CONSTRAINT IF EXISTS profiles_tiktok_user_id_key;
            EXCEPTION
                WHEN undefined_object THEN null;
            END $$;
        """))
        
        # Drop old unique constraint on tiktok_post_id if it exists
        session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE posts DROP CONSTRAINT IF EXISTS posts_tiktok_post_id_key;
            EXCEPTION
                WHEN undefined_object THEN null;
            END $$;
        """))
        
        # Add new composite unique constraints
        session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE profiles 
                ADD CONSTRAINT uq_profile_username_platform UNIQUE (username, platform);
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        
        session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE profiles 
                ADD CONSTRAINT uq_profile_platform_id UNIQUE (platform_user_id, platform);
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        
        session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE posts 
                ADD CONSTRAINT uq_post_platform_id UNIQUE (platform_post_id, platform);
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        
        # Create new indexes
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_profiles_platform ON profiles(platform)
        """))
        
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_profiles_active_platform ON profiles(is_active, platform)
        """))
        
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_posts_platform ON posts(platform)
        """))
        
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_alert_logs_platform ON alert_logs(platform)
        """))
        
        session.commit()
        
        # =================================================================
        # VERIFY MIGRATION
        # =================================================================
        logger.info("📊 Verifying migration...")
        
        # Count existing records
        result = session.execute(text("SELECT COUNT(*) FROM profiles WHERE platform = 'tiktok'"))
        profile_count = result.scalar()
        
        result = session.execute(text("SELECT COUNT(*) FROM posts WHERE platform = 'tiktok'"))
        post_count = result.scalar()
        
        logger.info(f"   ✅ Profiles migrated: {profile_count}")
        logger.info(f"   ✅ Posts migrated: {post_count}")
        
        logger.info("🎉 Migration v0.0.2 completed successfully!")
        logger.info("   All existing TikTok data preserved with platform='tiktok'")
        
        return True
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Migration failed: {e}")
        raise
    finally:
        session.close()


def rollback_migration():
    """Rollback the migration (for development/testing only)."""
    
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    
    database_url = get_database_url()
    logger.warning("⚠️ Rolling back migration v0.0.2...")
    
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # This is a simplified rollback - in production, you'd want more careful handling
        
        # Drop new columns from profiles
        session.execute(text("ALTER TABLE profiles DROP COLUMN IF EXISTS platform"))
        session.execute(text("ALTER TABLE profiles DROP COLUMN IF EXISTS user_role"))
        session.execute(text("ALTER TABLE profiles DROP COLUMN IF EXISTS subreddit_name"))
        session.execute(text("ALTER TABLE profiles DROP COLUMN IF EXISTS subreddit_subscribers"))
        session.execute(text("ALTER TABLE profiles DROP COLUMN IF EXISTS active_users"))
        
        # Rename platform_user_id back to tiktok_user_id
        result = session.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'profiles' AND column_name = 'platform_user_id'
        """))
        if result.fetchone():
            session.execute(text("ALTER TABLE profiles RENAME COLUMN platform_user_id TO tiktok_user_id"))
        
        # Drop new columns from posts
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS platform"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS retweet_count"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS quote_count"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS bookmark_count"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS impression_count"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS upvote_ratio"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS is_crosspost"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS original_subreddit"))
        session.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS reddit_score"))
        
        # Rename platform_post_id back to tiktok_post_id
        result = session.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'posts' AND column_name = 'platform_post_id'
        """))
        if result.fetchone():
            session.execute(text("ALTER TABLE posts RENAME COLUMN platform_post_id TO tiktok_post_id"))
        
        session.commit()
        logger.info("✅ Rollback completed")
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Rollback failed: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pulse Database Migration v0.0.2")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    args = parser.parse_args()
    
    if args.rollback:
        rollback_migration()
    else:
        run_migration()

