"""
Pulse - TikTok Analytics Dashboard
TikTok RapidAPI Client (Async) - tiktok-api23 by Lundehund

Updated for tiktok-api23 API endpoints:
- /api/user/info - Fetch user profile information
- /api/user/posts - Fetch user's videos with engagement stats

Features:
- Mandatory 2.0s pre-request delay for rate limit compliance
- Automatic retry with generous backoff for 429 rate limits: 5s → 10s → 20s
- Global asyncio.Lock to prevent concurrent API calls across processes
- Rate limit header logging (X-RateLimit-Reset, Retry-After)
- Configurable max retries (default: 3)
- Robust internal IP rotation (tiktok-api23 handles IP management)

API Documentation: https://rapidapi.com/lundehund/api/tiktok-api23
Host: tiktok-api23.p.rapidapi.com
"""

import httpx
import logging
import asyncio
from typing import Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from config import config

logger = logging.getLogger(__name__)

# Global lock to prevent concurrent API calls across all TikTokClient instances
# This ensures web and worker processes don't hit the API simultaneously
_global_api_lock = asyncio.Lock()


class RateLimitError(Exception):
    """Exception for rate limit errors after all retries exhausted."""
    pass


@dataclass
class TikTokProfile:
    """Parsed TikTok profile data."""
    user_id: str
    username: str
    display_name: str
    bio: str
    avatar_url: str
    follower_count: int
    following_count: int
    total_likes: int
    video_count: int


@dataclass  
class TikTokPost:
    """Parsed TikTok post data."""
    post_id: str
    description: str
    video_url: str
    thumbnail_url: str
    duration_seconds: int
    view_count: int
    like_count: int
    comment_count: int
    share_count: int
    posted_at: datetime


class TikTokAPIError(Exception):
    """Custom exception for TikTok API errors."""
    pass


class TikTokClient:
    """
    TikTok RapidAPI client using tiktok-api23 by Lundehund.

    Optimized for tiktok-api23 with robust IP rotation and reliable endpoints.

    API Configuration:
        - Host: tiktok-api23.p.rapidapi.com (set via RAPIDAPI_HOST env var)
        - Endpoints: /api/user/info, /api/user/posts
        - Rate Limiting: 2.0s pre-request delay + exponential backoff

    Usage:
        client = TikTokClient()

        # Fetch profile (returns user info including secUid)
        profile = await client.fetch_profile("username")

        # Fetch posts using secUid (from profile)
        posts = await client.fetch_recent_posts_by_id(profile.user_id)

        # Or fetch posts directly by username
        posts, sec_uid = await client.fetch_recent_posts("username")

    Field Mappings (tiktok-api23 → Database):
        - play_count → view_count
        - digg_count → like_count
        - comment_count → comment_count
        - share_count → share_count
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_host: Optional[str] = None
    ):
        self.api_key = api_key or config.RAPIDAPI_KEY
        self.api_host = api_host or config.RAPIDAPI_HOST
        self.base_url = f"https://{self.api_host}"
        
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }
    
    async def _make_request(
        self,
        endpoint: str,
        params: dict,
        timeout: float = 30.0,
        max_retries: int = 3
    ) -> dict:
        """
        Make authenticated request to RapidAPI with comprehensive rate limiting.

        IMPORTANT: ScrapTik Basic Plan Rate Limit Strategy:
        1. Mandatory 2.0s delay BEFORE each request (prevents bursts)
        2. Global asyncio.Lock (prevents concurrent requests)
        3. Generous retry backoff: 5s → 10s → 20s (allows API gateway reset)
        4. Rate limit header logging (X-RateLimit-Reset, Retry-After)

        Args:
            endpoint: API endpoint path
            params: Query parameters
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts for 429 errors (default: 3)

        Returns:
            JSON response data

        Raises:
            RateLimitError: After exhausting all retries on 429 errors
            TikTokAPIError: For other API errors
        """
        url = f"{self.base_url}{endpoint}"

        # Acquire global lock to prevent concurrent API calls
        async with _global_api_lock:
            # MANDATORY PRE-REQUEST DELAY: Ensures 1 request per 2 seconds minimum
            # This prevents bursting and stays well within Basic plan limits
            logger.debug(f"⏱️  Pre-request throttle: Sleeping 2.0s before {endpoint}")
            await asyncio.sleep(2.0)

            for attempt in range(max_retries + 1):
                async with httpx.AsyncClient() as client:
                    try:
                        response = await client.get(
                            url,
                            headers=self.headers,
                            params=params,
                            timeout=timeout
                        )

                        logger.debug(f"API Request: {url} | Status: {response.status_code}")

                        # Handle 202 with validation errors (tiktok-api23 specific)
                        if response.status_code == 202:
                            # tiktok-api23 returns 202 for validation errors
                            try:
                                response_data = response.json()
                                success = response_data.get("success", True)

                                if not success:
                                    error_msg = response_data.get("error", response_data.get("message", "Unknown validation error"))
                                    logger.error(f"❌ API validation error (202): {error_msg}")
                                    logger.debug(f"Full response: {response_data}")
                                    raise TikTokAPIError(f"API validation error: {error_msg}")
                            except ValueError:
                                # Can't parse JSON, treat as error
                                raise TikTokAPIError(f"Unexpected 202 response: {response.text}")

                        # Handle 429 Rate Limit with generous backoff
                        if response.status_code == 429:
                            # Log rate limit headers if available
                            retry_after = response.headers.get('Retry-After')
                            reset_time = response.headers.get('X-RateLimit-Reset')

                            if retry_after:
                                logger.warning(f"🚫 Rate limit response header: Retry-After = {retry_after}s")
                            if reset_time:
                                logger.warning(f"🚫 Rate limit response header: X-RateLimit-Reset = {reset_time}")

                            if attempt < max_retries:
                                # Generous backoff: 5s, 10s, 20s (gives API gateway time to reset)
                                backoff_seconds = 5 * (2 ** attempt)  # 5, 10, 20
                                logger.warning(
                                    f"⚠️  Rate limit hit on {endpoint} "
                                    f"(attempt {attempt + 1}/{max_retries + 1}). "
                                    f"Retrying in {backoff_seconds}s to allow API gateway reset..."
                                )
                                await asyncio.sleep(backoff_seconds)
                                continue
                            else:
                                logger.error(
                                    f"❌ Rate limit exhausted after {max_retries + 1} attempts on {endpoint}"
                                )
                                raise RateLimitError(
                                    f"Rate limit exceeded after {max_retries + 1} attempts. "
                                    f"Please wait before making more requests."
                                )

                        # Handle other error codes
                        if response.status_code == 404:
                            raise TikTokAPIError(f"Endpoint not found: {endpoint}")

                        if response.status_code == 401:
                            raise TikTokAPIError("Invalid API key. Check your RAPIDAPI_KEY.")

                        if response.status_code != 200:
                            raise TikTokAPIError(
                                f"API request failed with status {response.status_code}: {response.text}"
                            )

                        # Success - log if this was a retry
                        if attempt > 0:
                            logger.info(f"✅ Request succeeded after {attempt + 1} attempts")

                        return response.json()

                    except httpx.TimeoutException:
                        raise TikTokAPIError(f"Request timeout for {endpoint}")
                    except httpx.HTTPError as e:
                        raise TikTokAPIError(f"HTTP error: {str(e)}")

            # Should never reach here due to loop logic, but added for safety
            raise RateLimitError(f"Unexpected retry loop exit on {endpoint}")
    
    # =========================================================================
    # FETCH PROFILE (tiktok-api23: /api/user/info)
    # =========================================================================

    async def fetch_profile(self, username: str) -> TikTokProfile:
        """
        Fetch TikTok profile data by username using tiktok-api23.

        Args:
            username: TikTok handle (without @)

        Returns:
            TikTokProfile dataclass with user info (includes secUid)
        """
        # Sanitize input: strip @ symbol, whitespace, and convert to lowercase
        username = username.lstrip("@").strip().lower()

        logger.info(f"Fetching profile for @{username} via tiktok-api23...")

        # tiktok-api23 requires parameter named 'uniqueId', not 'username'
        params = {"uniqueId": username}
        data = await self._make_request("/api/user/info", params)

        try:
            # tiktok-api23 response structure - handle multiple possible formats
            # Format 1: data.userInfo.user.secUid
            # Format 2: data.data.user.secUid

            # Try both possible response structures
            user_info = data.get("userInfo") or data.get("data", {})
            user = user_info.get("user", {}) if isinstance(user_info, dict) else {}
            stats = user_info.get("stats", {}) if isinstance(user_info, dict) else {}

            # If user is empty, try alternative path
            if not user:
                user = user_info if isinstance(user_info, dict) else {}

            # CRITICAL: Extract secUid for posts endpoint
            # Try all possible locations according to Lundehund docs
            sec_uid = (
                user.get("secUid") or
                user.get("sec_uid") or
                user_info.get("secUid") or
                user_info.get("sec_uid") or
                data.get("secUid") or
                data.get("userInfo", {}).get("user", {}).get("secUid") or
                data.get("data", {}).get("user", {}).get("secUid") or
                ""
            )

            # Debug logging to trace extraction
            logger.debug(f"🔍 Response structure for @{username}:")
            logger.debug(f"  - data keys: {list(data.keys())}")
            logger.debug(f"  - user_info keys: {list(user_info.keys()) if isinstance(user_info, dict) else 'N/A'}")
            logger.debug(f"  - user keys: {list(user.keys()) if isinstance(user, dict) else 'N/A'}")

            # Defensive check: secUid is required for fetching posts
            if not sec_uid:
                logger.error(f"❌ Failed to extract secUid from profile response for @{username}")
                logger.error(f"Full response data: {data}")
                raise TikTokAPIError(
                    f"Missing secUid in profile response for @{username}. "
                    f"Cannot fetch posts without secUid."
                )

            logger.debug(f"🔍 Found secUid: {sec_uid}")
            logger.info(f"✅ Extracted secUid for @{username}: {sec_uid[:20]}...")

            # Extract avatar URL
            avatar_url = ""
            avatar_data = user.get("avatarLarger") or user.get("avatarMedium") or {}
            if isinstance(avatar_data, str):
                avatar_url = avatar_data
            elif isinstance(avatar_data, list) and avatar_data:
                avatar_url = avatar_data[0]

            profile = TikTokProfile(
                user_id=sec_uid,  # Store secUid in user_id field
                username=user.get("uniqueId", username),
                display_name=user.get("nickname", ""),
                bio=user.get("signature", ""),
                avatar_url=avatar_url,
                follower_count=int(stats.get("followerCount", 0)),
                following_count=int(stats.get("followingCount", 0)),
                total_likes=int(stats.get("heartCount", stats.get("heart", 0))),
                video_count=int(stats.get("videoCount", 0))
            )

            logger.info(f"✅ Profile fetched: @{profile.username} | {profile.follower_count:,} followers")
            return profile

        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse profile response: {e}")
            logger.debug(f"Response data: {data}")
            raise TikTokAPIError(f"Failed to parse profile data for @{username}")
    
    # =========================================================================
    # FETCH POSTS (tiktok-api23: /api/user/posts)
    # =========================================================================

    async def fetch_recent_posts_by_id(
        self,
        user_id: str,
        max_posts: int = 30,
        days_back: int = 30
    ) -> list[TikTokPost]:
        """
        Fetch recent posts using secUid via tiktok-api23.

        IMPORTANT: user_id parameter must be a valid secUid (not numeric user_id).

        Args:
            user_id: TikTok secUid (obtained from profile fetch)
            max_posts: Maximum number of posts to fetch
            days_back: Only include posts from last N days

        Returns:
            List of TikTokPost dataclasses

        Raises:
            TikTokAPIError: If secUid is empty or invalid
        """
        # Defensive check: secUid cannot be empty
        if not user_id or not str(user_id).strip():
            logger.error("❌ Cannot fetch posts: secUid is empty")
            raise TikTokAPIError(
                "secUid is required to fetch posts. "
                "Make sure to fetch profile first to obtain secUid."
            )

        sec_uid = str(user_id).strip()
        logger.debug(f"🔍 Found secUid: {sec_uid}")
        logger.info(f"Fetching posts for secUid: {sec_uid[:20]}... (tiktok-api23)")

        # tiktok-api23 requires exact parameter name 'secUid'
        params = {
            "secUid": sec_uid,
            "count": min(max_posts, 35)  # API limit
        }
        logger.debug(f"🔍 Posts API params: {params}")

        data = await self._make_request("/api/user/posts", params)

        return self._parse_posts_response(data, days_back)

    async def fetch_recent_posts(
        self,
        username: str,
        max_posts: int = 30,
        days_back: int = 30,
        cached_user_id: Optional[str] = None
    ) -> tuple[list[TikTokPost], str]:
        """
        Fetch recent posts by username via tiktok-api23.

        IMPORTANT: This method requires secUid. If not provided via cached_user_id,
        it will fetch the profile first to obtain secUid.

        Args:
            username: TikTok handle (without @)
            max_posts: Maximum number of posts to fetch
            days_back: Only include posts from last N days
            cached_user_id: Optional cached secUid (for efficiency - skips profile fetch)

        Returns:
            Tuple of (posts_list, secUid)
        """
        # Sanitize input: strip @ symbol, whitespace, and convert to lowercase
        username = username.lstrip("@").strip().lower()

        # Determine secUid
        if cached_user_id and str(cached_user_id).strip():
            # Use cached secUid (efficient path)
            sec_uid = str(cached_user_id).strip()
            logger.info(f"Using cached secUid for @{username}: {sec_uid[:20]}...")
        else:
            # No cached secUid - need to fetch profile first
            logger.info(f"No cached secUid for @{username}, fetching profile first...")
            profile = await self.fetch_profile(username)
            sec_uid = profile.user_id

            if not sec_uid:
                raise TikTokAPIError(
                    f"Failed to obtain secUid for @{username}. "
                    f"Cannot fetch posts without secUid."
                )

        # Now fetch posts using secUid
        posts = await self.fetch_recent_posts_by_id(sec_uid, max_posts, days_back)

        return posts, sec_uid
    
    def _parse_posts_response(self, data: dict, days_back: int = 30) -> list[TikTokPost]:
        """
        Parse posts from tiktok-api23 API response.

        Maps tiktok-api23 fields:
        - play_count → view_count
        - digg_count → like_count
        - comment_count → comment_count
        - share_count → share_count
        """

        # tiktok-api23 response structure
        posts_list = data.get("data", {}).get("videos", [])

        if not isinstance(posts_list, list):
            logger.warning(f"Unexpected posts response format: {type(posts_list)}")
            posts_list = []

        logger.info(f"📦 Received {len(posts_list)} posts from API")

        # DEBUG: If we got 0 posts, log the response structure to diagnose the issue
        if len(posts_list) == 0:
            logger.warning("⚠️  Received 0 posts from API - debugging response structure")
            logger.debug(f"🔍 Full response top-level keys: {list(data.keys())}")

            # Check what's in data.data
            data_inner = data.get("data", {})
            if isinstance(data_inner, dict):
                logger.debug(f"🔍 data.data keys: {list(data_inner.keys())}")

                # Try alternative paths for posts
                # Path 1: data.data.itemList (common in TikTok APIs)
                if "itemList" in data_inner:
                    logger.info("🔍 Found posts at data.data.itemList, using that instead")
                    posts_list = data_inner["itemList"]
                    logger.info(f"📦 Found {len(posts_list) if isinstance(posts_list, list) else 0} posts at itemList")

                # Path 2: data.data.aweme_list (older TikTok API format)
                elif "aweme_list" in data_inner:
                    logger.info("🔍 Found posts at data.data.aweme_list, using that instead")
                    posts_list = data_inner["aweme_list"]
                    logger.info(f"📦 Found {len(posts_list) if isinstance(posts_list, list) else 0} posts at aweme_list")

                # Path 3: Check if data.data itself is the list
                elif isinstance(data_inner, list):
                    logger.info("🔍 data.data is directly a list, using that")
                    posts_list = data_inner
                    logger.info(f"📦 Found {len(posts_list)} posts in direct list")

                else:
                    # Log sample of what we actually got
                    logger.warning(f"⚠️  Could not find posts in expected locations")
                    logger.debug(f"🔍 Sample of data.data content (first 500 chars): {str(data_inner)[:500]}")

            # Path 4: Check if videos is at root level
            elif "videos" in data:
                logger.info("🔍 Found posts at root level data.videos, using that instead")
                posts_list = data["videos"]
                logger.info(f"📦 Found {len(posts_list) if isinstance(posts_list, list) else 0} posts at root videos")

            # Path 5: Check if data itself is the list
            elif isinstance(data.get("data"), list):
                logger.info("🔍 data is directly a list, using that")
                posts_list = data["data"]
                logger.info(f"📦 Found {len(posts_list)} posts in data as list")

            else:
                logger.error("❌ Could not find posts array in any expected location")
                logger.debug(f"🔍 Full response (first 1000 chars): {str(data)[:1000]}")

        cutoff_date = datetime.now(datetime.UTC) - timedelta(days=days_back)
        posts = []

        for idx, item in enumerate(posts_list):
            try:
                # Parse timestamp (tiktok-api23 uses createTime)
                create_time = item.get("createTime", item.get("create_time", 0))

                # DEBUG: Log raw timestamp from first post
                if idx == 0:
                    logger.debug(f"🔍 Raw timestamp from first post: {create_time} (type: {type(create_time)})")
                    logger.debug(f"🔍 Full first post keys: {list(item.keys())}")

                if isinstance(create_time, str):
                    try:
                        posted_at = datetime.fromisoformat(create_time.replace("Z", "+00:00"))
                    except Exception as e:
                        logger.warning(f"Failed to parse ISO timestamp '{create_time}': {e}")
                        posted_at = datetime.now(datetime.UTC)
                else:
                    # Unix timestamp
                    try:
                        posted_at = datetime.fromtimestamp(int(create_time), tz=datetime.UTC) if create_time else datetime.now(datetime.UTC)
                    except Exception as e:
                        logger.warning(f"Failed to parse Unix timestamp '{create_time}': {e}")
                        posted_at = datetime.now(datetime.UTC)

                # TEMPORARILY DISABLED: Skip old posts for debugging
                # if posted_at < cutoff_date:
                #     logger.debug(f"Skipping post from {posted_at} (older than cutoff {cutoff_date})")
                #     continue

                # Extract stats (tiktok-api23 structure)
                stats = item.get("stats", {})

                # Map tiktok-api23 fields to our database schema
                post = TikTokPost(
                    post_id=str(item.get("id", item.get("aweme_id", ""))),
                    description=item.get("desc", item.get("description", "")),
                    video_url=self._extract_video_url(item),
                    thumbnail_url=self._extract_thumbnail(item),
                    duration_seconds=int(item.get("duration", item.get("video", {}).get("duration", 0))),
                    view_count=int(stats.get("play_count", stats.get("playCount", 0))),  # MAPPED: play_count
                    like_count=int(stats.get("digg_count", stats.get("diggCount", 0))),  # MAPPED: digg_count
                    comment_count=int(stats.get("comment_count", stats.get("commentCount", 0))),  # MAPPED: comment_count
                    share_count=int(stats.get("share_count", stats.get("shareCount", 0))),
                    posted_at=posted_at
                )

                posts.append(post)

                # DEBUG: Log first post details
                if idx == 0:
                    logger.debug(f"🔍 First post parsed: ID={post.post_id}, views={post.view_count}, date={post.posted_at}")

            except Exception as e:
                logger.warning(f"Failed to parse post {idx}: {e}")
                logger.debug(f"Problematic post data: {item}")
                continue

        # Sort by posted date (newest first)
        posts.sort(key=lambda p: p.posted_at, reverse=True)

        logger.info(f"✅ Parsed {len(posts)} posts (date filter temporarily disabled for debugging)")

        if posts:
            logger.info(f"   Newest post: {posts[0].posted_at}, Oldest post: {posts[-1].posted_at}")

        return posts
    
    def _extract_video_url(self, item: dict) -> str:
        """
        Extract video URL from tiktok-api23 post data.

        tiktok-api23 provides direct video URLs in the response.
        """
        # Try direct video URL first (tiktok-api23 format)
        video_url = item.get("videoUrl", item.get("video_url", ""))
        if video_url:
            return video_url

        # Fallback to nested structure
        video = item.get("video", {})
        if isinstance(video, dict):
            # Check for direct playAddr
            play_addr = video.get("playAddr", video.get("play_addr", ""))
            if isinstance(play_addr, str) and play_addr:
                return play_addr

            # Check for nested url_list structure
            if isinstance(play_addr, dict):
                url_list = play_addr.get("url_list", [])
                if url_list and isinstance(url_list, list):
                    return url_list[0]

        return ""

    def _extract_thumbnail(self, item: dict) -> str:
        """
        Extract thumbnail URL from tiktok-api23 post data.

        tiktok-api23 provides cover images in multiple formats.
        """
        # Try direct cover URL first (tiktok-api23 format)
        cover_url = item.get("coverUrl", item.get("cover_url", ""))
        if cover_url:
            return cover_url

        # Try dynamicCover (animated thumbnail)
        dynamic_cover = item.get("dynamicCover", item.get("dynamic_cover", ""))
        if dynamic_cover:
            return dynamic_cover

        # Fallback to nested video structure
        video = item.get("video", {})
        if isinstance(video, dict):
            cover = video.get("cover", video.get("originCover", {}))

            if isinstance(cover, str) and cover:
                return cover

            if isinstance(cover, dict):
                url_list = cover.get("url_list", [])
                if url_list and isinstance(url_list, list):
                    return url_list[0]

        return ""
    
    async def health_check(self) -> bool:
        """Test API connectivity."""
        try:
            await self.fetch_profile("tiktok")
            return True
        except TikTokAPIError:
            return False
