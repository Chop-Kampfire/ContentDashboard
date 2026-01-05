"""
Pulse - TikTok Analytics Dashboard
Main Scraper Module

Updated for ScrapTik API 2-step process:
1. Fetch profile (gets user_id) OR use cached user_id
2. Fetch posts using user_id (NOT username)

This module orchestrates:
1. Fetching TikTok profile and post data via RapidAPI
2. Upserting data into PostgreSQL (saves user_id for efficiency)
3. Detecting viral posts (views > 5x average)
4. Sending Telegram alerts for viral content

Usage:
    # Add a new profile to watchlist
    await add_profile_to_watchlist("username")
    
    # Update all profiles (called by background worker)
    await update_all_profiles()
    
    # Update single profile
    await update_profile("username")
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)


class TikTokScraper:
    """
    Main scraper class that coordinates data fetching, storage, and alerts.
    
    Uses ScrapTik 2-step process:
    1. Get profile (extracts user_id) 
    2. Fetch posts by user_id (saves API calls by caching user_id)
    """
    
    def __init__(self):
        self.tiktok = TikTokClient()
        self.telegram = TelegramNotifier()
        self.viral_threshold = config.VIRAL_THRESHOLD_MULTIPLIER
        self.lookback_days = config.POSTS_LOOKBACK_DAYS
    
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
                logger.warning(f"Profile @{username} already exists, updating instead")
                return await self.update_profile(username)
        
        # Step 1: Fetch profile (gets user_id)
        try:
            profile_data = await self.tiktok.fetch_profile(username)
            user_id = profile_data.user_id
            
            logger.info(f"📋 Got profile for @{username}, user_id: {user_id}")
            
            # Step 2: Fetch posts using user_id (correct ScrapTik method)
            posts_data = await self.tiktok.fetch_recent_posts_by_id(
                user_id=user_id,
                max_posts=50,
                days_back=self.lookback_days
            )
            
        except TikTokAPIError as e:
            logger.error(f"❌ Failed to fetch @{username}: {e}")
            raise
        
        # Calculate average views
        avg_views = self._calculate_average_views(posts_data)
        
        # Save to database (including user_id for future use)
        with get_db_context() as db:
            profile = Profile(
                tiktok_user_id=user_id,  # IMPORTANT: Save user_id
                username=profile_data.username,
                display_name=profile_data.display_name,
                bio=profile_data.bio,
                avatar_url=profile_data.avatar_url,
                follower_count=profile_data.follower_count,
                following_count=profile_data.following_count,
                total_likes=profile_data.total_likes,
                video_count=profile_data.video_count,
                average_post_views=avg_views,
                last_scraped_at=datetime.utcnow()
            )
            db.add(profile)
            db.flush()  # Get the ID
            
            # Create initial history record
            history = ProfileHistory(
                profile_id=profile.id,
                follower_count=profile.follower_count,
                following_count=profile.following_count,
                total_likes=profile.total_likes,
                video_count=profile.video_count,
                follower_change=0,
                likes_change=0,
                recorded_at=datetime.utcnow()
            )
            db.add(history)
            
            # Insert posts
            for post_data in posts_data:
                post = self._create_post_record(profile.id, post_data, avg_views)
                db.add(post)
            
            db.commit()
            
            # Capture values BEFORE session closes (fix for detached instance error)
            # #region agent log
            saved_username = profile.username
            saved_follower_count = profile.follower_count
            saved_id = profile.id
            import json; open(r'c:\Users\tyron\OneDrive\Documents\Kampfire Vibez\ContentDashboard\.cursor\debug.log','a').write(json.dumps({"hypothesisId":"A","location":"scraper.py:add_profile:captured_values","message":"Captured values before expunge","data":{"saved_username":saved_username,"saved_id":saved_id},"timestamp":__import__('time').time()*1000})+'\n')
            # #endregion
            
            logger.info(
                f"✅ Added @{username} | "
                f"user_id: {user_id} | "
                f"{profile.follower_count:,} followers | "
                f"{len(posts_data)} posts | "
                f"Avg views: {avg_views:,.0f}"
            )
            
            # Send welcome notification
            if send_notification:
                try:
                    await self.telegram.send_welcome_alert(
                        username=saved_username,  # Use captured value
                        follower_count=saved_follower_count  # Use captured value
                    )
                except Exception as e:
                    logger.warning(f"Failed to send welcome notification: {e}")
            
            # Expunge the profile from session so it can be used after context closes
            db.expunge(profile)
            
            # #region agent log
            import json; open(r'c:\Users\tyron\OneDrive\Documents\Kampfire Vibez\ContentDashboard\.cursor\debug.log','a').write(json.dumps({"hypothesisId":"A","location":"scraper.py:add_profile:after_expunge","message":"Profile expunged, returning","data":{"profile_id":profile.id},"timestamp":__import__('time').time()*1000})+'\n')
            # #endregion
            
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
            # Step 1: Fetch profile (also updates user_id if it changed)
            profile_data = await self.tiktok.fetch_profile(username)
            user_id = profile_data.user_id
            
            # Step 2: Fetch posts using user_id
            # Use cached user_id if available, otherwise use the one from profile fetch
            effective_user_id = cached_user_id or user_id
            
            if cached_user_id:
                logger.info(f"Using cached user_id for @{username}: {cached_user_id}")
            else:
                logger.info(f"No cached user_id, using fetched: {user_id}")
            
            posts_data = await self.tiktok.fetch_recent_posts_by_id(
                user_id=effective_user_id,
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
            
            # Update profile metrics (including user_id in case it changed)
            profile.tiktok_user_id = user_id  # Always update to latest
            profile.display_name = profile_data.display_name
            profile.bio = profile_data.bio
            profile.avatar_url = profile_data.avatar_url
            profile.follower_count = profile_data.follower_count
            profile.following_count = profile_data.following_count
            profile.total_likes = profile_data.total_likes
            profile.video_count = profile_data.video_count
            profile.average_post_views = new_avg_views
            profile.last_scraped_at = datetime.utcnow()
            
            # Create history snapshot
            history = ProfileHistory(
                profile_id=profile.id,
                follower_count=profile.follower_count,
                following_count=profile.following_count,
                total_likes=profile.total_likes,
                video_count=profile.video_count,
                follower_change=profile.follower_count - old_follower_count,
                likes_change=profile.total_likes - old_likes,
                recorded_at=datetime.utcnow()
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
            
            # Use cached user_id for posts (skip username-to-id conversion!)
            if cached_user_id:
                logger.info(f"📦 Using cached user_id: {cached_user_id}")
                posts_data = await self.tiktok.fetch_recent_posts_by_id(
                    user_id=cached_user_id,
                    max_posts=50,
                    days_back=self.lookback_days
                )
            else:
                # No cached ID, need to do full 2-step process
                logger.info(f"⚠️ No cached user_id, fetching with username")
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
            
            # Update all fields
            profile.tiktok_user_id = cached_user_id or profile_data.user_id
            profile.display_name = profile_data.display_name
            profile.bio = profile_data.bio
            profile.avatar_url = profile_data.avatar_url
            profile.follower_count = profile_data.follower_count
            profile.following_count = profile_data.following_count
            profile.total_likes = profile_data.total_likes
            profile.video_count = profile_data.video_count
            profile.average_post_views = new_avg_views
            profile.last_scraped_at = datetime.utcnow()
            
            # History snapshot
            history = ProfileHistory(
                profile_id=profile.id,
                follower_count=profile.follower_count,
                following_count=profile.following_count,
                total_likes=profile.total_likes,
                video_count=profile.video_count,
                follower_change=profile.follower_count - old_follower_count,
                likes_change=profile.total_likes - old_likes,
                recorded_at=datetime.utcnow()
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
            return 0.0
        
        view_counts = [p.view_count for p in posts if p.view_count > 0]
        return mean(view_counts) if view_counts else 0.0
    
    def _create_post_record(
        self,
        profile_id: int,
        post_data: TikTokPost,
        avg_views: float
    ) -> Post:
        """Create a Post database record from TikTok post data."""
        is_viral = post_data.view_count > (avg_views * self.viral_threshold)
        
        return Post(
            profile_id=profile_id,
            tiktok_post_id=post_data.post_id,
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
            existing.updated_at = datetime.utcnow()
            
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
# CONVENIENCE FUNCTIONS (for importing)
# =============================================================================

async def add_profile_to_watchlist(username: str) -> Profile:
    """Add a TikTok profile to the watchlist."""
    scraper = TikTokScraper()
    return await scraper.add_profile(username)


async def update_profile(username: str) -> Optional[Profile]:
    """Update a single profile."""
    scraper = TikTokScraper()
    return await scraper.update_profile(username)


async def update_all_profiles() -> dict:
    """Update all active profiles."""
    scraper = TikTokScraper()
    return await scraper.update_all_profiles()


async def remove_profile(username: str) -> bool:
    """Remove a profile from watchlist."""
    scraper = TikTokScraper()
    return await scraper.remove_profile(username)


# =============================================================================
# CLI ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import sys
    
    async def main():
        if len(sys.argv) < 2:
            print("Usage: python scraper.py <command> [args]")
            print("Commands:")
            print("  add <username>     - Add profile to watchlist")
            print("  update <username>  - Update single profile")
            print("  update-all         - Update all profiles")
            print("  remove <username>  - Remove profile from watchlist")
            return
        
        command = sys.argv[1].lower()
        
        if command == "add" and len(sys.argv) >= 3:
            username = sys.argv[2]
            await add_profile_to_watchlist(username)
            
        elif command == "update" and len(sys.argv) >= 3:
            username = sys.argv[2]
            await update_profile(username)
            
        elif command == "update-all":
            await update_all_profiles()
            
        elif command == "remove" and len(sys.argv) >= 3:
            username = sys.argv[2]
            await remove_profile(username)
            
        else:
            print(f"Unknown command: {command}")
    
    asyncio.run(main())
