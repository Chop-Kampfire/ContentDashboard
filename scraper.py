"""
Pulse - Multi-Platform Analytics Dashboard
Main Scraper Module

Multi-Platform Architecture:
- ScraperFactory: Returns appropriate scraper based on platform type
- TikTokScraper: Fully implemented with ScrapTik API 2-step process
- TwitterScraper: Stub for future implementation
- RedditScraper: Stub for future implementation

This module orchestrates:
1. Fetching profile and post data from multiple platforms
2. Upserting data into PostgreSQL with platform-specific columns
3. Detecting viral content (platform-specific thresholds)
4. Sending Telegram alerts for viral content

Usage:
    # Get platform-specific scraper
    scraper = ScraperFactory.get_scraper('tiktok')

    # Add a new profile to watchlist
    await scraper.add_profile("username")

    # Update all profiles (called by background worker)
    await update_all_profiles()
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Protocol
from statistics import mean

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import config
from database import (
    get_db_context,
    Profile,
    ProfileHistory,
    Post,
    PostHistory,
    AlertLog
)
from services.tiktok_client import TikTokClient, TikTokProfile, TikTokPost, TikTokAPIError
from services.telegram_notifier import TelegramNotifier
from services.logger import get_logger, log_scrape_result, log_viral_alert

# Get logger for this module
logger = get_logger(__name__)


# =============================================================================
# ABSTRACT BASE SCRAPER
# =============================================================================

class BaseScraper(ABC):
    """
    Abstract base class for platform-specific scrapers.

    Each platform scraper must implement these core methods for
    fetching profiles, posts, and managing the watchlist.
    """

    def __init__(self):
        self.telegram = TelegramNotifier()
        self.viral_threshold = config.VIRAL_THRESHOLD_MULTIPLIER
        self.lookback_days = config.POSTS_LOOKBACK_DAYS

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Return platform identifier (e.g., 'tiktok', 'twitter', 'reddit')."""
        pass

    @abstractmethod
    async def add_profile(self, username: str, send_notification: bool = True) -> Profile:
        """Add a new profile to the watchlist."""
        pass

    @abstractmethod
    async def update_profile(self, username: str) -> Optional[Profile]:
        """Update an existing profile with fresh data."""
        pass

    @abstractmethod
    async def update_profile_by_id(self, profile_id: int) -> Optional[Profile]:
        """Update a profile using database ID."""
        pass

    async def remove_profile(self, username: str) -> bool:
        """
        Soft-delete a profile from the watchlist (platform-agnostic).

        Args:
            username: Platform handle

        Returns:
            True if removed, False if not found
        """
        username = username.lstrip("@").strip().lower()

        with get_db_context() as db:
            profile = db.query(Profile).filter(
                Profile.username == username,
                Profile.platform == self.platform_name
            ).first()
            if profile:
                profile.is_active = False
                db.commit()
                logger.info(f"🗑️ Removed @{username} ({self.platform_name}) from watchlist")
                return True

        return False


class TikTokScraper(BaseScraper):
    """
    TikTok-specific scraper implementation.

    Uses ScrapTik 2-step process:
    1. Get profile (extracts user_id)
    2. Fetch posts by user_id (saves API calls by caching user_id)
    """

    @property
    def platform_name(self) -> str:
        return 'tiktok'

    def __init__(self):
        super().__init__()
        self.tiktok = TikTokClient()
    
    # =========================================================================
    # PUBLIC METHODS
    # =========================================================================
    
    async def add_profile(self, username: str, send_notification: bool = True) -> Profile:
        """
        Add a new TikTok profile to the watchlist.
        
        Process:
        1. Fetch profile data (gets user_id)
        2. Fetch posts using user_id
        3. Save all data including user_id for future efficiency
        
        Args:
            username: TikTok handle (without @)
            send_notification: Send Telegram welcome notification
            
        Returns:
            Created Profile database object
        """
        username = username.lstrip("@").strip().lower()
        logger.info(f"📥 Adding new profile: @{username}")
        
        # Check if already exists
        with get_db_context() as db:
            existing = db.query(Profile).filter(Profile.username == username).first()
            if existing:
                # Reactivate if it was soft-deleted
                if not existing.is_active:
                    existing.is_active = True
                    db.commit()
                    logger.info(f"Profile @{username} reactivated")
                
                logger.warning(f"Profile @{username} already exists, updating instead")
                return await self.update_profile(username)
        
        # Step 1: Fetch profile (gets secUid)
        try:
            profile_data = await self.tiktok.fetch_profile(username)
            user_id = profile_data.user_id  # This is the secUid from tiktok-api23

            logger.debug(f"🔍 Found secUid: {user_id}")
            logger.info(f"📋 Got profile for @{username}, secUid: {user_id[:20] if user_id else 'N/A'}...")

            # Step 2: Fetch posts using secUid (tiktok-api23 method)
            posts_data = await self.tiktok.fetch_recent_posts_by_id(
                user_id=user_id,  # Pass secUid
                max_posts=50,
                days_back=self.lookback_days
            )
            
        except TikTokAPIError as e:
            logger.error(f"❌ Failed to fetch @{username}: {e}")
            raise
        
        # Calculate average views
        avg_views = self._calculate_average_views(posts_data)

        logger.info(f"📊 Calculated avg views: {avg_views:,.0f} from {len(posts_data)} posts")

        # Save to database (including secUid for future use)
        with get_db_context() as db:
            profile = Profile(
                tiktok_user_id=user_id,  # IMPORTANT: Save secUid for subsequent API calls
                username=profile_data.username,
                display_name=profile_data.display_name,
                bio=profile_data.bio,
                avatar_url=profile_data.avatar_url,
                follower_count=profile_data.follower_count,
                following_count=profile_data.following_count,
                total_likes=profile_data.total_likes,
                video_count=profile_data.video_count,
                average_post_views=avg_views,
                last_scraped_at=datetime.now(datetime.UTC)
            )
            db.add(profile)
            db.flush()  # Get the ID

            logger.info(f"💾 Saved profile with ID: {profile.id}")

            # Create initial history record
            history = ProfileHistory(
                profile_id=profile.id,
                follower_count=profile.follower_count,
                following_count=profile.following_count,
                total_likes=profile.total_likes,
                video_count=profile.video_count,
                follower_change=0,
                likes_change=0,
                recorded_at=datetime.now(datetime.UTC)
            )
            db.add(history)

            # Insert posts
            logger.info(f"💾 Saving {len(posts_data)} posts to database...")
            posts_saved = 0
            for post_data in posts_data:
                post = self._create_post_record(profile.id, post_data, avg_views)
                db.add(post)
                posts_saved += 1

            logger.debug(f"   Added {posts_saved} post records to session")

            db.commit()

            logger.info(f"✅ Database commit successful - profile and {posts_saved} posts saved")

            # Capture values BEFORE session closes (fix for detached instance error)
            saved_username = profile.username
            saved_follower_count = profile.follower_count

            logger.info(
                f"✅ Added @{username} | "
                f"secUid: {user_id[:20]}... | "
                f"{profile.follower_count:,} followers | "
                f"{len(posts_data)} posts | "
                f"Avg views: {avg_views:,.0f}"
            )
            
            # Send welcome notification
            if send_notification:
                try:
                    await self.telegram.send_welcome_alert(
                        username=saved_username,
                        follower_count=saved_follower_count
                    )
                except Exception as e:
                    logger.warning(f"Failed to send welcome notification: {e}")
            
            # Expunge the profile from session so it can be used after context closes
            db.expunge(profile)
            
            return profile
    
    async def update_profile(self, username: str) -> Optional[Profile]:
        """
        Update an existing profile with fresh data from TikTok.
        
        Uses cached user_id from database to skip username-to-id conversion.
        
        Args:
            username: TikTok handle (without @)
            
        Returns:
            Updated Profile object or None if not found
        """
        username = username.lstrip("@").strip().lower()
        logger.info(f"🔄 Updating profile: @{username}")
        
        # Get existing profile with cached user_id
        with get_db_context() as db:
            profile = db.query(Profile).filter(Profile.username == username).first()
            if not profile:
                logger.warning(f"Profile @{username} not found in database")
                return None
            
            profile_id = profile.id
            cached_user_id = profile.tiktok_user_id  # Use cached user_id!
            old_follower_count = profile.follower_count
            old_likes = profile.total_likes
            old_avg_views = profile.average_post_views
        
        # Fetch fresh data
        try:
            # Step 1: Fetch profile (also updates secUid if it changed)
            profile_data = await self.tiktok.fetch_profile(username)
            user_id = profile_data.user_id  # This is the secUid

            logger.debug(f"🔍 Found secUid: {user_id}")

            # Step 2: Fetch posts using secUid
            # Use cached secUid if available, otherwise use the one from profile fetch
            effective_user_id = cached_user_id or user_id

            if cached_user_id:
                logger.info(f"Using cached secUid for @{username}: {cached_user_id[:20]}...")
            else:
                logger.info(f"No cached secUid, using fetched: {user_id[:20]}...")

            posts_data = await self.tiktok.fetch_recent_posts_by_id(
                user_id=effective_user_id,  # Pass secUid
                max_posts=50,
                days_back=self.lookback_days
            )
            
        except TikTokAPIError as e:
            logger.error(f"❌ Failed to update @{username}: {e}")
            return None
        
        # Calculate new average views
        new_avg_views = self._calculate_average_views(posts_data)
        
        with get_db_context() as db:
            profile = db.query(Profile).filter(Profile.id == profile_id).first()

            # Update profile metrics (including secUid in case it changed)
            profile.tiktok_user_id = user_id  # Always update to latest secUid
            profile.display_name = profile_data.display_name
            profile.bio = profile_data.bio
            profile.avatar_url = profile_data.avatar_url
            profile.follower_count = profile_data.follower_count
            profile.following_count = profile_data.following_count
            profile.total_likes = profile_data.total_likes
            profile.video_count = profile_data.video_count
            profile.average_post_views = new_avg_views
            profile.last_scraped_at = datetime.now(datetime.UTC)
            
            # Create history snapshot
            history = ProfileHistory(
                profile_id=profile.id,
                follower_count=profile.follower_count,
                following_count=profile.following_count,
                total_likes=profile.total_likes,
                video_count=profile.video_count,
                follower_change=profile.follower_count - old_follower_count,
                likes_change=profile.total_likes - old_likes,
                recorded_at=datetime.now(datetime.UTC)
            )
            db.add(history)
            
            # UPSERT posts and check for viral content
            viral_posts = []
            for post_data in posts_data:
                is_new, is_viral, post = await self._upsert_post(
                    db, profile.id, post_data, old_avg_views
                )
                if is_viral:
                    viral_posts.append((post_data, post))
            
            db.commit()
            
            logger.info(
                f"✅ Updated @{username} | "
                f"Followers: {profile.follower_count:,} ({profile.follower_count - old_follower_count:+,}) | "
                f"Viral posts detected: {len(viral_posts)}"
            )
            
            # Send viral alerts
            for post_data, post_record in viral_posts:
                await self._send_viral_alert(
                    db, profile, post_data, post_record, old_avg_views
                )
            
            # Expunge profile so it can be accessed after session closes
            db.expunge(profile)
            
            return profile
    
    async def update_profile_by_id(self, profile_id: int) -> Optional[Profile]:
        """
        Update a profile using database ID and cached user_id.
        
        More efficient for bulk updates as it uses saved user_id directly.
        
        Args:
            profile_id: Database profile ID
            
        Returns:
            Updated Profile object or None if not found
        """
        # Get profile with cached data
        with get_db_context() as db:
            profile = db.query(Profile).filter(Profile.id == profile_id).first()
            if not profile:
                logger.warning(f"Profile ID {profile_id} not found")
                return None
            
            username = profile.username
            cached_user_id = profile.tiktok_user_id
            old_follower_count = profile.follower_count
            old_likes = profile.total_likes
            old_avg_views = profile.average_post_views
        
        logger.info(f"🔄 Updating profile: @{username} (ID: {profile_id})")
        
        try:
            # Fetch fresh profile data
            profile_data = await self.tiktok.fetch_profile(username)
            user_id = profile_data.user_id  # This is the secUid

            logger.debug(f"🔍 Found secUid: {user_id}")

            # Use cached secUid for posts (more efficient)
            if cached_user_id:
                logger.info(f"📦 Using cached secUid: {cached_user_id[:20]}...")
                posts_data = await self.tiktok.fetch_recent_posts_by_id(
                    user_id=cached_user_id,  # Pass secUid
                    max_posts=50,
                    days_back=self.lookback_days
                )
            else:
                # No cached secUid, need to do full 2-step process
                logger.info(f"⚠️ No cached secUid, fetching with username")
                posts_data, new_user_id = await self.tiktok.fetch_recent_posts(
                    username=username,
                    max_posts=50,
                    days_back=self.lookback_days
                )
                cached_user_id = new_user_id
                
        except TikTokAPIError as e:
            logger.error(f"❌ Failed to update @{username}: {e}")
            return None
        
        # Calculate new average
        new_avg_views = self._calculate_average_views(posts_data)

        with get_db_context() as db:
            profile = db.query(Profile).filter(Profile.id == profile_id).first()

            # Update all fields (including secUid)
            profile.tiktok_user_id = cached_user_id or user_id  # Store secUid
            profile.display_name = profile_data.display_name
            profile.bio = profile_data.bio
            profile.avatar_url = profile_data.avatar_url
            profile.follower_count = profile_data.follower_count
            profile.following_count = profile_data.following_count
            profile.total_likes = profile_data.total_likes
            profile.video_count = profile_data.video_count
            profile.average_post_views = new_avg_views
            profile.last_scraped_at = datetime.now(datetime.UTC)
            
            # History snapshot
            history = ProfileHistory(
                profile_id=profile.id,
                follower_count=profile.follower_count,
                following_count=profile.following_count,
                total_likes=profile.total_likes,
                video_count=profile.video_count,
                follower_change=profile.follower_count - old_follower_count,
                likes_change=profile.total_likes - old_likes,
                recorded_at=datetime.now(datetime.UTC)
            )
            db.add(history)
            
            # UPSERT posts
            viral_posts = []
            for post_data in posts_data:
                is_new, is_viral, post = await self._upsert_post(
                    db, profile.id, post_data, old_avg_views
                )
                if is_viral:
                    viral_posts.append((post_data, post))
            
            db.commit()
            
            logger.info(
                f"✅ Updated @{username} | "
                f"Followers: {profile.follower_count:,} ({profile.follower_count - old_follower_count:+,}) | "
                f"Viral: {len(viral_posts)}"
            )
            
            # Send viral alerts
            for post_data, post_record in viral_posts:
                await self._send_viral_alert(
                    db, profile, post_data, post_record, old_avg_views
                )
            
            # Expunge profile so it can be accessed after session closes
            db.expunge(profile)
            
            return profile
    
    async def update_all_profiles(self) -> dict:
        """
        Update all active profiles in the watchlist.
        
        Uses cached user_ids for efficiency (saves 1 API call per profile).
        Called by the background worker every 6 hours.
        
        Returns:
            Dict with success/failure counts
        """
        logger.info("🔄 Starting bulk update for all profiles...")
        
        # Get all active profile IDs and usernames
        with get_db_context() as db:
            profiles = db.query(Profile).filter(Profile.is_active == True).all()
            profile_data = [(p.id, p.username, p.tiktok_user_id) for p in profiles]
        
        results = {"success": 0, "failed": 0, "viral_alerts": 0}
        
        for profile_id, username, user_id in profile_data:
            try:
                if user_id:
                    # Use efficient method with cached user_id
                    await self.update_profile_by_id(profile_id)
                else:
                    # Fallback to username-based update
                    await self.update_profile(username)
                    
                results["success"] += 1
            except Exception as e:
                logger.error(f"Failed to update @{username}: {e}")
                results["failed"] += 1
            
            # Rate limiting - wait between requests
            await asyncio.sleep(2)
        
        logger.info(
            f"✅ Bulk update complete: "
            f"{results['success']} success, {results['failed']} failed"
        )
        
        return results
    
    async def remove_profile(self, username: str) -> bool:
        """
        Soft-delete a profile from the watchlist.
        
        Args:
            username: TikTok handle
            
        Returns:
            True if removed, False if not found
        """
        username = username.lstrip("@").strip().lower()
        
        with get_db_context() as db:
            profile = db.query(Profile).filter(Profile.username == username).first()
            if profile:
                profile.is_active = False
                db.commit()
                logger.info(f"🗑️ Removed @{username} from watchlist")
                return True
        
        return False
    
    # =========================================================================
    # PRIVATE HELPER METHODS
    # =========================================================================
    
    def _calculate_average_views(self, posts: list[TikTokPost]) -> float:
        """Calculate mean view count from list of posts."""
        if not posts:
            logger.warning("⚠️  No posts provided for average calculation")
            return 0.0

        view_counts = [p.view_count for p in posts if p.view_count > 0]

        if not view_counts:
            logger.warning(f"⚠️  All {len(posts)} posts have 0 views - average will be 0")
            return 0.0

        avg = mean(view_counts)
        logger.debug(f"📊 Average calculation: {len(view_counts)} posts with views, avg = {avg:,.0f}")
        return avg
    
    def _create_post_record(
        self,
        profile_id: int,
        post_data: TikTokPost,
        avg_views: float
    ) -> Post:
        """Create a Post database record from TikTok post data."""
        is_viral = post_data.view_count > (avg_views * self.viral_threshold)

        post = Post(
            profile_id=profile_id,
            platform='tiktok',  # Set platform for multi-platform support
            platform_post_id=post_data.post_id,  # Use platform_post_id for consistency
            tiktok_post_id=post_data.post_id,  # Keep legacy column for backward compatibility
            description=post_data.description,
            video_url=post_data.video_url,
            thumbnail_url=post_data.thumbnail_url,
            duration_seconds=post_data.duration_seconds,
            view_count=post_data.view_count,
            like_count=post_data.like_count,
            comment_count=post_data.comment_count,
            share_count=post_data.share_count,
            is_viral=is_viral,
            viral_alert_sent=False,
            posted_at=post_data.posted_at
        )

        logger.debug(f"   📝 Created post record: {post_data.post_id[:15]}... views={post_data.view_count:,}, viral={is_viral}")

        return post
    
    async def _upsert_post(
        self,
        db,
        profile_id: int,
        post_data: TikTokPost,
        avg_views: float
    ) -> tuple[bool, bool, Post]:
        """
        Insert or update a post record.
        
        Returns:
            Tuple of (is_new, is_viral, post_record)
        """
        is_viral = post_data.view_count > (avg_views * self.viral_threshold)
        
        # Check if post exists
        existing = db.query(Post).filter(
            Post.tiktok_post_id == post_data.post_id
        ).first()
        
        if existing:
            # Update existing post
            old_views = existing.view_count
            existing.view_count = post_data.view_count
            existing.like_count = post_data.like_count
            existing.comment_count = post_data.comment_count
            existing.share_count = post_data.share_count
            existing.updated_at = datetime.now(datetime.UTC)
            
            # Check if newly viral (wasn't before, is now)
            if is_viral and not existing.is_viral:
                existing.is_viral = True
            
            # Only trigger alert if:
            # 1. Post is viral
            # 2. Alert hasn't been sent yet
            should_alert = is_viral and not existing.viral_alert_sent
            
            # Add history record if views changed significantly (>10%)
            if old_views > 0 and abs(post_data.view_count - old_views) / old_views > 0.1:
                history = PostHistory(
                    post_id=existing.id,
                    view_count=post_data.view_count,
                    like_count=post_data.like_count,
                    comment_count=post_data.comment_count,
                    share_count=post_data.share_count
                )
                db.add(history)
            
            return (False, should_alert, existing)
        
        else:
            # Insert new post
            post = self._create_post_record(profile_id, post_data, avg_views)
            db.add(post)
            db.flush()
            
            # New posts that are viral should trigger alert
            return (True, is_viral, post)
    
    async def _send_viral_alert(
        self,
        db,
        profile: Profile,
        post_data: TikTokPost,
        post_record: Post,
        avg_views: float
    ):
        """Send Telegram alert for viral post and log it."""
        try:
            result = await self.telegram.send_viral_alert(
                username=profile.username,
                post_id=post_data.post_id,
                views=post_data.view_count,
                avg_views=avg_views,
                description=post_data.description,
                video_url=post_data.video_url
            )
            
            success = result.get("ok", False)
            
            # Mark alert as sent
            post_record.viral_alert_sent = True
            
            # Log the alert
            alert_log = AlertLog(
                post_id=post_record.id,
                profile_id=profile.id,
                alert_type="viral_post",
                message=f"Viral alert for @{profile.username} - {post_data.view_count:,} views ({post_data.view_count/avg_views:.1f}x avg)",
                success=success,
                error_message=None if success else str(result.get("error"))
            )
            db.add(alert_log)
            
            logger.info(f"🚀 Viral alert sent for @{profile.username} post {post_data.post_id}")
            
        except Exception as e:
            logger.error(f"Failed to send viral alert: {e}")
            
            # Log failed alert
            alert_log = AlertLog(
                post_id=post_record.id,
                profile_id=profile.id,
                alert_type="viral_post",
                message=f"Failed viral alert for @{profile.username}",
                success=False,
                error_message=str(e)
            )
            db.add(alert_log)


# =============================================================================
# TWITTER SCRAPER (Stub for Future Implementation)
# =============================================================================

class TwitterScraper(BaseScraper):
    """
    Twitter/X scraper stub.

    TODO: Implement Twitter API integration:
    - Twitter API v2 client
    - Tweet fetching with engagement metrics (retweets, quotes, bookmarks, impressions)
    - Twitter-specific viral detection logic
    """

    @property
    def platform_name(self) -> str:
        return 'twitter'

    async def add_profile(self, username: str, send_notification: bool = True) -> Profile:
        raise NotImplementedError(
            "Twitter scraper not yet implemented. "
            "Add Twitter API client in services/twitter_client.py first."
        )

    async def update_profile(self, username: str) -> Optional[Profile]:
        raise NotImplementedError("Twitter scraper not yet implemented.")

    async def update_profile_by_id(self, profile_id: int) -> Optional[Profile]:
        raise NotImplementedError("Twitter scraper not yet implemented.")


# =============================================================================
# REDDIT SCRAPER (Stub for Future Implementation)
# =============================================================================

class RedditScraper(BaseScraper):
    """
    Reddit scraper stub.

    TODO: Implement Reddit API integration via PRAW:
    - Reddit API client (PRAW wrapper)
    - Subreddit post fetching with Reddit-specific metrics (score, upvote_ratio, crossposts)
    - Support for user_role: 'user' (regular user) vs 'subreddit' (community tracking)
    - Viral detection based on subreddit average score
    """

    @property
    def platform_name(self) -> str:
        return 'reddit'

    async def add_profile(self, username: str, send_notification: bool = True) -> Profile:
        raise NotImplementedError(
            "Reddit scraper not yet implemented. "
            "Add Reddit API client in services/reddit_client.py first."
        )

    async def update_profile(self, username: str) -> Optional[Profile]:
        raise NotImplementedError("Reddit scraper not yet implemented.")

    async def update_profile_by_id(self, profile_id: int) -> Optional[Profile]:
        raise NotImplementedError("Reddit scraper not yet implemented.")


# =============================================================================
# SCRAPER FACTORY
# =============================================================================

class ScraperFactory:
    """
    Factory for creating platform-specific scrapers.

    Usage:
        scraper = ScraperFactory.get_scraper('tiktok')
        profile = await scraper.add_profile('username')

    Supported platforms:
        - 'tiktok': TikTokScraper (fully implemented)
        - 'twitter': TwitterScraper (stub)
        - 'reddit': RedditScraper (stub)
    """

    _scrapers = {
        'tiktok': TikTokScraper,
        'twitter': TwitterScraper,
        'reddit': RedditScraper,
    }

    @classmethod
    def get_scraper(cls, platform: str) -> BaseScraper:
        """
        Get the appropriate scraper for a platform.

        Args:
            platform: Platform identifier ('tiktok', 'twitter', 'reddit')

        Returns:
            Platform-specific scraper instance

        Raises:
            ValueError: If platform is not supported
        """
        platform = platform.lower().strip()

        if platform not in cls._scrapers:
            supported = ', '.join(cls._scrapers.keys())
            raise ValueError(
                f"Unsupported platform: '{platform}'. "
                f"Supported platforms: {supported}"
            )

        scraper_class = cls._scrapers[platform]
        return scraper_class()

    @classmethod
    def get_supported_platforms(cls) -> list[str]:
        """Get list of all supported platform identifiers."""
        return list(cls._scrapers.keys())


# =============================================================================
# CONVENIENCE FUNCTIONS (for importing)
# =============================================================================

async def add_profile_to_watchlist(username: str, platform: str = 'tiktok') -> Profile:
    """
    Add a profile to the watchlist.

    Args:
        username: Platform handle (without @)
        platform: Platform type ('tiktok', 'twitter', 'reddit'). Defaults to 'tiktok'.

    Returns:
        Created Profile database object
    """
    scraper = ScraperFactory.get_scraper(platform)
    return await scraper.add_profile(username)


async def update_profile(username: str, platform: str = 'tiktok') -> Optional[Profile]:
    """
    Update a single profile.

    Args:
        username: Platform handle
        platform: Platform type ('tiktok', 'twitter', 'reddit'). Defaults to 'tiktok'.

    Returns:
        Updated Profile object or None if not found
    """
    scraper = ScraperFactory.get_scraper(platform)
    return await scraper.update_profile(username)


async def update_all_profiles() -> dict:
    """
    Update all active profiles across all platforms.

    Returns:
        Dict with success/failure counts per platform
    """
    logger.info("🔄 Starting bulk update for all profiles (multi-platform)...")

    # Get all active profiles grouped by platform
    with get_db_context() as db:
        profiles = db.query(Profile).filter(Profile.is_active == True).all()
        platform_profiles = {}
        for p in profiles:
            platform = p.platform or 'tiktok'  # Default to tiktok for legacy records
            if platform not in platform_profiles:
                platform_profiles[platform] = []
            platform_profiles[platform].append((p.id, p.username))

    results = {"success": 0, "failed": 0, "by_platform": {}}

    # Update each platform separately
    for platform, profile_list in platform_profiles.items():
        logger.info(f"📊 Updating {len(profile_list)} {platform} profiles...")
        results["by_platform"][platform] = {"success": 0, "failed": 0}

        try:
            scraper = ScraperFactory.get_scraper(platform)
        except ValueError as e:
            logger.error(f"Unsupported platform '{platform}': {e}")
            results["failed"] += len(profile_list)
            results["by_platform"][platform]["failed"] = len(profile_list)
            continue

        for profile_id, username in profile_list:
            try:
                await scraper.update_profile_by_id(profile_id)
                results["success"] += 1
                results["by_platform"][platform]["success"] += 1
            except NotImplementedError:
                logger.warning(f"Skipping {platform} profile @{username} (not implemented)")
                results["failed"] += 1
                results["by_platform"][platform]["failed"] += 1
            except Exception as e:
                logger.error(f"Failed to update @{username} ({platform}): {e}")
                results["failed"] += 1
                results["by_platform"][platform]["failed"] += 1

            # Rate limiting - wait between requests
            await asyncio.sleep(2)

    logger.info(
        f"✅ Bulk update complete: "
        f"{results['success']} success, {results['failed']} failed"
    )

    return results


async def remove_profile(username: str, platform: str = 'tiktok') -> bool:
    """
    Remove a profile from watchlist.

    Args:
        username: Platform handle
        platform: Platform type ('tiktok', 'twitter', 'reddit'). Defaults to 'tiktok'.

    Returns:
        True if removed, False if not found
    """
    scraper = ScraperFactory.get_scraper(platform)
    return await scraper.remove_profile(username)


# =============================================================================
# CLI ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import sys

    async def main():
        if len(sys.argv) < 2:
            supported = ', '.join(ScraperFactory.get_supported_platforms())
            print("Usage: python scraper.py <command> [args]")
            print("\nCommands:")
            print("  add <username> [platform]     - Add profile to watchlist")
            print("  update <username> [platform]  - Update single profile")
            print("  update-all                    - Update all profiles (multi-platform)")
            print("  remove <username> [platform]  - Remove profile from watchlist")
            print(f"\nSupported platforms: {supported}")
            print("Default platform: tiktok")
            return

        command = sys.argv[1].lower()

        if command == "add" and len(sys.argv) >= 3:
            username = sys.argv[2]
            platform = sys.argv[3] if len(sys.argv) >= 4 else 'tiktok'
            await add_profile_to_watchlist(username, platform)

        elif command == "update" and len(sys.argv) >= 3:
            username = sys.argv[2]
            platform = sys.argv[3] if len(sys.argv) >= 4 else 'tiktok'
            await update_profile(username, platform)

        elif command == "update-all":
            await update_all_profiles()

        elif command == "remove" and len(sys.argv) >= 3:
            username = sys.argv[2]
            platform = sys.argv[3] if len(sys.argv) >= 4 else 'tiktok'
            await remove_profile(username, platform)

        else:
            print(f"Unknown command: {command}")

    asyncio.run(main())
