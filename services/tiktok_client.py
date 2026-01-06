"""
Pulse - TikTok Analytics Dashboard
TikTok RapidAPI Client (Async)

Updated for ScrapTik API with correct 2-step process:
1. /username-to-id - Convert username to numeric user_id
2. /user-posts - Fetch posts using user_id (NOT username)

Features:
- Automatic retry with exponential backoff for 429 rate limits
- Configurable max retries (default: 3)
- Intelligent backoff: 2s → 4s → 8s
- Detailed logging of rate limit events

API Documentation: https://rapidapi.com/scraptik-api-scraptik-api-default/api/scraptik
"""

import httpx
import logging
import asyncio
from typing import Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from config import config

logger = logging.getLogger(__name__)


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
    TikTok RapidAPI client for fetching profile and post data.
    
    Optimized for ScrapTik API with 2-step post fetching process.
    
    Usage:
        client = TikTokClient()
        
        # Get profile (also returns user_id)
        profile = await client.fetch_profile("username")
        
        # Fetch posts using user_id (more efficient)
        posts = await client.fetch_recent_posts_by_id(profile.user_id)
        
        # Or use username (will auto-convert)
        posts = await client.fetch_recent_posts("username")
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
        Make authenticated request to RapidAPI with automatic retry on rate limits.

        IMPORTANT: Includes mandatory 1.5s delay after successful requests to comply
        with Basic plan rate limits (strict per-second limits).

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

                    # Handle 429 Rate Limit with exponential backoff
                    if response.status_code == 429:
                        if attempt < max_retries:
                            backoff_seconds = 2 ** attempt  # 2s, 4s, 8s
                            logger.warning(
                                f"⚠️  Rate limit hit on {endpoint} "
                                f"(attempt {attempt + 1}/{max_retries + 1}). "
                                f"Retrying in {backoff_seconds}s..."
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

                    response_data = response.json()

                    # MANDATORY DELAY: Basic plan has strict per-second rate limits
                    # Sleep 1.5s after every successful API call to stay under limits
                    logger.debug(f"⏱️  Rate limit compliance: Sleeping 1.5s after {endpoint}")
                    await asyncio.sleep(1.5)

                    return response_data

                except httpx.TimeoutException:
                    raise TikTokAPIError(f"Request timeout for {endpoint}")
                except httpx.HTTPError as e:
                    raise TikTokAPIError(f"HTTP error: {str(e)}")

        # Should never reach here due to loop logic, but added for safety
        raise RateLimitError(f"Unexpected retry loop exit on {endpoint}")
    
    # =========================================================================
    # STEP 1: USERNAME TO USER ID
    # =========================================================================
    
    async def username_to_id(self, username: str) -> str:
        """
        Convert a TikTok username to numeric user_id.
        
        Args:
            username: TikTok handle (without @)
            
        Returns:
            str: Numeric TikTok user_id
        """
        username = username.lstrip("@").strip().lower()
        
        logger.info(f"Converting @{username} to user_id...")
        
        data = await self._make_request("/username-to-id", {"username": username})
        
        # Extract user_id from response
        user_id = (
            data.get("user_id") or 
            data.get("uid") or 
            data.get("id") or
            data.get("user", {}).get("id") or
            data.get("user", {}).get("uid") or
            data.get("data", {}).get("user_id")
        )
        
        if not user_id:
            raise TikTokAPIError(f"Could not extract user_id for @{username}")
        
        user_id = str(user_id)
        logger.info(f"✅ @{username} → user_id: {user_id}")
        
        return user_id
    
    # =========================================================================
    # FETCH PROFILE
    # =========================================================================
    
    async def fetch_profile(self, username: str) -> TikTokProfile:
        """
        Fetch TikTok profile data by username.
        
        Args:
            username: TikTok handle (without @)
            
        Returns:
            TikTokProfile dataclass with user info (includes user_id)
        """
        username = username.lstrip("@").strip().lower()
        
        data = await self._make_request("/get-user", {"username": username})
        
        try:
            user = data.get("user", data)
            
            # Extract avatar URL
            avatar_url = ""
            avatar_data = user.get("avatar_larger", user.get("avatarLarger", {}))
            if isinstance(avatar_data, dict):
                url_list = avatar_data.get("url_list", [])
                avatar_url = url_list[0] if url_list else ""
            elif isinstance(avatar_data, str):
                avatar_url = avatar_data
            
            return TikTokProfile(
                user_id=str(user.get("uid", user.get("id", user.get("user_id", "")))),
                username=user.get("unique_id", user.get("uniqueId", username)),
                display_name=user.get("nickname", ""),
                bio=user.get("signature", ""),
                avatar_url=avatar_url,
                follower_count=int(user.get("follower_count", user.get("followerCount", 0))),
                following_count=int(user.get("following_count", user.get("followingCount", 0))),
                total_likes=int(user.get("total_favorited", user.get("heartCount", user.get("heart", 0)))),
                video_count=int(user.get("aweme_count", user.get("videoCount", 0)))
            )
            
        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse profile response: {e}")
            raise TikTokAPIError(f"Failed to parse profile data for @{username}")
    
    # =========================================================================
    # STEP 2: FETCH POSTS BY USER ID
    # =========================================================================
    
    async def fetch_recent_posts_by_id(
        self,
        user_id: str,
        max_posts: int = 30,
        days_back: int = 30
    ) -> list[TikTokPost]:
        """
        Fetch recent posts using numeric user_id.
        
        This is the correct method for ScrapTik API.
        
        Args:
            user_id: Numeric TikTok user ID
            max_posts: Maximum number of posts to fetch
            days_back: Only include posts from last N days
            
        Returns:
            List of TikTokPost dataclasses
        """
        params = {
            "user_id": str(user_id),
            "count": min(max_posts, 35)  # ScrapTik max
        }
        
        data = await self._make_request("/user-posts", params)
        
        return self._parse_posts_response(data, days_back)
    
    async def fetch_recent_posts(
        self,
        username: str,
        max_posts: int = 30,
        days_back: int = 30,
        cached_user_id: Optional[str] = None
    ) -> tuple[list[TikTokPost], str]:
        """
        Fetch recent posts with automatic username-to-id conversion.
        
        Args:
            username: TikTok handle (without @)
            max_posts: Maximum number of posts to fetch
            days_back: Only include posts from last N days
            cached_user_id: Optional cached user_id to skip conversion
            
        Returns:
            Tuple of (posts_list, user_id) - save user_id for efficiency
        """
        username = username.lstrip("@").strip().lower()
        
        # Use cached user_id or convert
        if cached_user_id:
            user_id = cached_user_id
            logger.info(f"Using cached user_id for @{username}: {user_id}")
        else:
            user_id = await self.username_to_id(username)
        
        posts = await self.fetch_recent_posts_by_id(user_id, max_posts, days_back)
        
        return posts, user_id
    
    def _parse_posts_response(self, data: dict, days_back: int = 30) -> list[TikTokPost]:
        """Parse posts from API response."""
        
        posts_list = (
            data.get("aweme_list") or
            data.get("itemList") or
            data.get("videos") or
            data.get("data", {}).get("aweme_list") or
            []
        )
        
        if not isinstance(posts_list, list):
            posts_list = []
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)
        posts = []
        
        for item in posts_list:
            try:
                # Parse timestamp
                create_time = item.get("create_time", item.get("createTime", 0))
                if isinstance(create_time, str):
                    try:
                        posted_at = datetime.fromisoformat(create_time.replace("Z", "+00:00"))
                    except:
                        posted_at = datetime.utcnow()
                else:
                    posted_at = datetime.utcfromtimestamp(int(create_time)) if create_time else datetime.utcnow()
                
                # Skip old posts
                if posted_at < cutoff_date:
                    continue
                
                # Extract stats
                stats = item.get("statistics", item.get("stats", {}))
                
                post = TikTokPost(
                    post_id=str(item.get("aweme_id", item.get("id", ""))),
                    description=item.get("desc", item.get("description", "")),
                    video_url=self._extract_video_url(item),
                    thumbnail_url=self._extract_thumbnail(item),
                    duration_seconds=int(item.get("duration", 0)),
                    view_count=int(stats.get("play_count", stats.get("playCount", 0))),
                    like_count=int(stats.get("digg_count", stats.get("diggCount", 0))),
                    comment_count=int(stats.get("comment_count", stats.get("commentCount", 0))),
                    share_count=int(stats.get("share_count", stats.get("shareCount", 0))),
                    posted_at=posted_at
                )
                
                posts.append(post)
                
            except Exception as e:
                logger.warning(f"Failed to parse post: {e}")
                continue
        
        # Sort by posted date (newest first)
        posts.sort(key=lambda p: p.posted_at, reverse=True)
        
        logger.info(f"Parsed {len(posts)} posts (last {days_back} days)")
        return posts
    
    def _extract_video_url(self, item: dict) -> str:
        """Extract video URL from post data."""
        video = item.get("video", {})
        if isinstance(video, dict):
            play_addr = video.get("play_addr", {})
            if isinstance(play_addr, dict):
                url_list = play_addr.get("url_list", [])
                if url_list:
                    return url_list[0]
            return video.get("playAddr", "")
        return ""
    
    def _extract_thumbnail(self, item: dict) -> str:
        """Extract thumbnail URL from post data."""
        video = item.get("video", {})
        if isinstance(video, dict):
            cover = video.get("cover", video.get("origin_cover", {}))
            if isinstance(cover, dict):
                url_list = cover.get("url_list", [])
                if url_list:
                    return url_list[0]
            elif isinstance(cover, str):
                return cover
        return ""
    
    async def health_check(self) -> bool:
        """Test API connectivity."""
        try:
            await self.fetch_profile("tiktok")
            return True
        except TikTokAPIError:
            return False
