"""
Pulse - Multi-Platform Analytics Dashboard
Database Models (SQLAlchemy ORM)
Version 0.0.3 - Railway Deployment with Automatic Migrations
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, DateTime, 
    ForeignKey, Float, Boolean, Index, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


# =============================================================================
# PLATFORM CONSTANTS
# =============================================================================

class Platform:
    """Supported social media platforms."""
    TIKTOK = "tiktok"
    TWITTER = "twitter"
    REDDIT = "reddit"


class UserRole:
    """User roles for analytics categorization."""
    CREATOR = "creator"
    MODERATOR = "moderator"
    POWER_USER = "power_user"
    BRAND = "brand"


# =============================================================================
# PROFILE MODEL
# =============================================================================

class Profile(Base):
    """
    Stores profile information for accounts on the watchlist.
    Supports TikTok, Twitter/X, and Reddit platforms.
    """
    __tablename__ = 'profiles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Platform identification
    platform = Column(String(32), nullable=False, default=Platform.TIKTOK, index=True)
    platform_user_id = Column(String(255), nullable=True)  # Platform's internal ID (secUid for TikTok)
    username = Column(String(64), nullable=False, index=True)  # @handle

    # Legacy column - kept for backwards compatibility during migration
    tiktok_user_id = Column(String(255), nullable=True)  # TikTok secUid (increased to 255 for long identifiers)
    
    # User categorization
    user_role = Column(String(32), nullable=True, default=UserRole.CREATOR)
    
    # Profile info (common across platforms)
    display_name = Column(String(128), nullable=True)
    bio = Column(Text, nullable=True)
    avatar_url = Column(Text, nullable=True)
    
    # Common metrics (latest snapshot)
    follower_count = Column(BigInteger, default=0)
    following_count = Column(BigInteger, default=0)
    total_likes = Column(BigInteger, default=0)  # Hearts/Likes/Karma
    video_count = Column(Integer, default=0)  # Videos/Tweets/Posts
    
    # Reddit-specific fields
    subreddit_name = Column(String(128), nullable=True)  # For subreddit profiles (r/name)
    subreddit_subscribers = Column(BigInteger, nullable=True)  # Subreddit subscriber count
    active_users = Column(Integer, nullable=True)  # Currently active users (Reddit)
    
    # Calculated metrics for alerts
    average_post_views = Column(Float, default=0.0)  # Rolling avg for viral detection
    
    # Tracking
    is_active = Column(Boolean, default=True)  # Soft delete / pause tracking
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_scraped_at = Column(DateTime, nullable=True)  # Last successful API fetch
    
    # Relationships
    posts = relationship("Post", back_populates="profile", cascade="all, delete-orphan")
    history = relationship("ProfileHistory", back_populates="profile", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Profile [{self.platform}] @{self.username} | {self.follower_count:,} followers>"


# =============================================================================
# PROFILE HISTORY MODEL
# =============================================================================

class ProfileHistory(Base):
    """
    Time-series data for tracking profile metrics over time.
    One record per profile per scrape cycle (every 6 hours).
    Powers the "investment-style" follower growth charts.
    """
    __tablename__ = 'profile_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey('profiles.id', ondelete='CASCADE'), nullable=False)
    
    # Snapshot of metrics at this point in time
    follower_count = Column(BigInteger, default=0)
    following_count = Column(BigInteger, default=0)
    total_likes = Column(BigInteger, default=0)
    video_count = Column(Integer, default=0)
    
    # Reddit-specific history
    subreddit_subscribers = Column(BigInteger, nullable=True)
    active_users = Column(Integer, nullable=True)
    
    # Calculated deltas (change since last record)
    follower_change = Column(Integer, default=0)  # +/- followers since last snapshot
    likes_change = Column(Integer, default=0)
    
    recorded_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationship
    profile = relationship("Profile", back_populates="history")

    # Composite index for efficient time-series queries
    __table_args__ = (
        Index('idx_profile_history_lookup', 'profile_id', 'recorded_at'),
    )

    def __repr__(self):
        return f"<ProfileHistory @{self.profile_id} | {self.recorded_at}>"


# =============================================================================
# POST MODEL
# =============================================================================

class Post(Base):
    """
    Individual posts/content for tracked profiles.
    Supports TikTok videos, Tweets, and Reddit posts.
    """
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey('profiles.id', ondelete='CASCADE'), nullable=False)
    
    # Platform identification
    platform = Column(String(32), nullable=False, default=Platform.TIKTOK, index=True)
    platform_post_id = Column(String(255), nullable=True, index=True)  # Platform's post ID (increased to 255 for long IDs)

    # Legacy column - kept for backwards compatibility during migration
    tiktok_post_id = Column(String(255), nullable=True)  # Increased to 255 for long identifiers
    
    # Content info (common across platforms)
    description = Column(Text, nullable=True)  # Caption/text/title
    video_url = Column(Text, nullable=True)  # Media URL
    thumbnail_url = Column(Text, nullable=True)
    duration_seconds = Column(Integer, nullable=True)  # For videos
    
    # Common engagement metrics (updated each scrape)
    view_count = Column(BigInteger, default=0)  # Views/Impressions
    like_count = Column(BigInteger, default=0)  # Likes/Hearts/Upvotes
    comment_count = Column(BigInteger, default=0)
    share_count = Column(BigInteger, default=0)  # Shares/Retweets
    
    # Twitter-specific fields
    retweet_count = Column(BigInteger, nullable=True)  # Number of retweets
    quote_count = Column(BigInteger, nullable=True)  # Number of quote tweets
    bookmark_count = Column(BigInteger, nullable=True)  # Number of bookmarks
    impression_count = Column(BigInteger, nullable=True)  # Number of impressions
    
    # Reddit-specific fields
    upvote_ratio = Column(Float, nullable=True)  # Upvote percentage (0.0-1.0)
    is_crosspost = Column(Boolean, nullable=True)  # Whether post is a crosspost
    original_subreddit = Column(String(128), nullable=True)  # Source subreddit if crosspost
    reddit_score = Column(Integer, nullable=True)  # Reddit score (upvotes - downvotes)
    
    # Viral detection
    is_viral = Column(Boolean, default=False)  # Flagged if views > 5x avg
    viral_alert_sent = Column(Boolean, default=False)  # Prevent duplicate Telegram alerts
    
    # Timestamps
    posted_at = Column(DateTime, nullable=True)  # When posted on platform
    created_at = Column(DateTime, default=datetime.utcnow)  # When we first scraped it
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    profile = relationship("Profile", back_populates="posts")

    def __repr__(self):
        post_id = self.platform_post_id or self.tiktok_post_id or "unknown"
        return f"<Post [{self.platform}] {post_id} | {self.view_count:,} views>"


# =============================================================================
# POST HISTORY MODEL
# =============================================================================

class PostHistory(Base):
    """
    Time-series tracking for individual post performance.
    Useful for seeing how a post's engagement grows over time.
    """
    __tablename__ = 'post_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey('posts.id', ondelete='CASCADE'), nullable=False)
    
    # Common metrics snapshot
    view_count = Column(BigInteger, default=0)
    like_count = Column(BigInteger, default=0)
    comment_count = Column(BigInteger, default=0)
    share_count = Column(BigInteger, default=0)
    
    # Twitter-specific metrics
    retweet_count = Column(BigInteger, nullable=True)
    quote_count = Column(BigInteger, nullable=True)
    
    # Reddit-specific metrics
    upvote_ratio = Column(Float, nullable=True)
    reddit_score = Column(Integer, nullable=True)
    
    recorded_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index('idx_post_history_lookup', 'post_id', 'recorded_at'),
    )

    def __repr__(self):
        return f"<PostHistory post={self.post_id} | {self.recorded_at}>"


# =============================================================================
# ALERT LOG MODEL
# =============================================================================

class AlertLog(Base):
    """
    Logs all Telegram alerts sent (for auditing and preventing duplicates).
    """
    __tablename__ = 'alert_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey('posts.id', ondelete='SET NULL'), nullable=True)
    profile_id = Column(Integer, ForeignKey('profiles.id', ondelete='SET NULL'), nullable=True)

    # Platform for context
    platform = Column(String(32), nullable=True)

    alert_type = Column(String(32), nullable=False)  # 'viral_post', 'milestone', etc.
    message = Column(Text, nullable=False)

    sent_at = Column(DateTime, default=datetime.utcnow)
    success = Column(Boolean, default=True)
    error_message = Column(Text, nullable=True)

    def __repr__(self):
        return f"<AlertLog {self.alert_type} | {self.sent_at}>"


# =============================================================================
# SUBREDDIT TRAFFIC MODEL
# =============================================================================

class SubredditTraffic(Base):
    """
    Stores daily Reddit subreddit traffic statistics.
    Fetched via PRAW (Reddit API) on a daily cron schedule.
    """
    __tablename__ = 'subreddit_traffic'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Subreddit identification
    subreddit_name = Column(String(128), nullable=False, index=True)

    # Traffic date (unique per subreddit per day)
    timestamp = Column(DateTime, nullable=False, index=True)

    # Traffic metrics
    unique_visitors = Column(Integer, default=0)  # Unique visitors for the day
    pageviews = Column(Integer, default=0)  # Total page views for the day
    subscriptions = Column(Integer, default=0)  # Net new subscribers for the day

    # Tracking
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Unique constraint on subreddit + date to support upsert pattern
    __table_args__ = (
        UniqueConstraint('subreddit_name', 'timestamp', name='uq_subreddit_traffic_date'),
        Index('idx_subreddit_traffic_lookup', 'subreddit_name', 'timestamp'),
    )

    def __repr__(self):
        return f"<SubredditTraffic r/{self.subreddit_name} | {self.timestamp} | {self.pageviews:,} views>"
