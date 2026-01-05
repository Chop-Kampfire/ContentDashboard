"""
Pulse - TikTok Analytics Dashboard
Database Models (SQLAlchemy ORM)
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, DateTime, 
    ForeignKey, Float, Boolean, Index, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Profile(Base):
    """
    Stores TikTok profile information for accounts on the watchlist.
    """
    __tablename__ = 'profiles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # TikTok identifiers
    tiktok_user_id = Column(String(64), unique=True, nullable=True)  # TikTok's internal ID
    username = Column(String(64), unique=True, nullable=False, index=True)  # @handle
    
    # Profile info
    display_name = Column(String(128), nullable=True)
    bio = Column(Text, nullable=True)
    avatar_url = Column(Text, nullable=True)
    
    # Current metrics (latest snapshot)
    follower_count = Column(BigInteger, default=0)
    following_count = Column(BigInteger, default=0)
    total_likes = Column(BigInteger, default=0)  # Likes received across all videos
    video_count = Column(Integer, default=0)
    
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
        return f"<Profile @{self.username} | {self.follower_count:,} followers>"


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


class Post(Base):
    """
    Individual TikTok posts/videos for tracked profiles.
    Stores performance metrics for each post.
    """
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey('profiles.id', ondelete='CASCADE'), nullable=False)
    
    # TikTok identifiers
    tiktok_post_id = Column(String(64), unique=True, nullable=False, index=True)
    
    # Content info
    description = Column(Text, nullable=True)  # Caption/text
    video_url = Column(Text, nullable=True)
    thumbnail_url = Column(Text, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    
    # Engagement metrics (updated each scrape)
    view_count = Column(BigInteger, default=0)
    like_count = Column(BigInteger, default=0)
    comment_count = Column(BigInteger, default=0)
    share_count = Column(BigInteger, default=0)
    
    # Viral detection
    is_viral = Column(Boolean, default=False)  # Flagged if views > 5x avg
    viral_alert_sent = Column(Boolean, default=False)  # Prevent duplicate Telegram alerts
    
    # Timestamps
    posted_at = Column(DateTime, nullable=True)  # When posted on TikTok
    created_at = Column(DateTime, default=datetime.utcnow)  # When we first scraped it
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    profile = relationship("Profile", back_populates="posts")

    # Index for efficient queries
    __table_args__ = (
        Index('idx_posts_profile_posted', 'profile_id', 'posted_at'),
        Index('idx_posts_viral', 'is_viral', 'viral_alert_sent'),
    )

    def __repr__(self):
        return f"<Post {self.tiktok_post_id} | {self.view_count:,} views>"


class PostHistory(Base):
    """
    Time-series tracking for individual post performance.
    Useful for seeing how a post's views grow over time.
    """
    __tablename__ = 'post_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey('posts.id', ondelete='CASCADE'), nullable=False)
    
    # Snapshot of metrics
    view_count = Column(BigInteger, default=0)
    like_count = Column(BigInteger, default=0)
    comment_count = Column(BigInteger, default=0)
    share_count = Column(BigInteger, default=0)
    
    recorded_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index('idx_post_history_lookup', 'post_id', 'recorded_at'),
    )

    def __repr__(self):
        return f"<PostHistory post={self.post_id} | {self.recorded_at}>"


class AlertLog(Base):
    """
    Logs all Telegram alerts sent (for auditing and preventing duplicates).
    """
    __tablename__ = 'alert_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey('posts.id', ondelete='SET NULL'), nullable=True)
    profile_id = Column(Integer, ForeignKey('profiles.id', ondelete='SET NULL'), nullable=True)
    
    alert_type = Column(String(32), nullable=False)  # 'viral_post', 'milestone', etc.
    message = Column(Text, nullable=False)
    
    sent_at = Column(DateTime, default=datetime.utcnow)
    success = Column(Boolean, default=True)
    error_message = Column(Text, nullable=True)

    def __repr__(self):
        return f"<AlertLog {self.alert_type} | {self.sent_at}>"

