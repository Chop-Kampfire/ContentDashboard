"""
Pulse - TikTok Analytics Dashboard
TikTok RapidAPI Client

Assumed API: "TikTok All In One" (tiktok-all-in-one.p.rapidapi.com)
Alternative: "Scraptik" or "TikTok Scraper" - adjust endpoints as needed.

API Documentation Reference:
- GET /user/info - Get user profile by username
- GET /user/posts - Get user's recent posts
"""

import httpx
import logging
from typing import Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from config import config

logger = logging.getLogger(__name__)


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
    
    Supports multiple RapidAPI TikTok providers with configurable endpoints.
    Default: "TikTok All In One" API
    
    Usage:
        client = TikTokClient()
        profile = await client.fetch_profile("username")
        posts = await client.fetch_recent_posts("username")
    """
    
    # =========================================================================
    # API ENDPOINT CONFIGURATION
    # Adjust these based on your specific RapidAPI TikTok provider
    # =========================================================================
    
    # Option 1: TikTok All In One
    ENDPOINTS = {
        "tiktok-all-in-one.p.rapidapi.com": {
            "profile": "/user/info",
            "posts": "/user/posts",
            "profile_param": "username",
            "posts_param": "username",
        },
        # Option 2: Scraptik API (alternative)
        "scraptik.p.rapidapi.com": {
            "profile": "/get-user",
            "posts": "/get-user-posts", 
            "profile_param": "username",
            "posts_param": "username",
        },
        # Option 3: TikTok Scraper API
        "tiktok-scraper7.p.rapidapi.com": {
            "profile": "/user/info",
            "posts": "/user/posts",
            "profile_param": "unique_id",
            "posts_param": "unique_id",
        }
    }
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_host: Optional[str] = None
    ):
        self.api_key = api_key or config.RAPIDAPI_KEY
        self.api_host = api_host or config.RAPIDAPI_HOST
        self.base_url = f"https://{self.api_host}"
        
        # Get endpoint config for this host
        self.endpoints = self.ENDPOINTS.get(
            self.api_host,
            self.ENDPOINTS["tiktok-all-in-one.p.rapidapi.com"]
        )
        
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }
    
    async def _make_request(
        self,
        endpoint: str,
        params: dict,
        timeout: float = 30.0
    ) -> dict:
        """Make authenticated request to RapidAPI."""
        
        url = f"{self.base_url}{endpoint}"
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=timeout
                )
                
                # Log for debugging
                logger.debug(f"API Request: {url} | Status: {response.status_code}")
                
                if response.status_code == 429:
                    raise TikTokAPIError("Rate limit exceeded. Please wait before retrying.")
                
                if response.status_code == 401:
                    raise TikTokAPIError("Invalid API key. Check your RAPIDAPI_KEY.")
                
                if response.status_code != 200:
                    raise TikTokAPIError(
                        f"API request failed with status {response.status_code}: {response.text}"
                    )
                
                return response.json()
                
            except httpx.TimeoutException:
                raise TikTokAPIError(f"Request timeout for {endpoint}")
            except httpx.HTTPError as e:
                raise TikTokAPIError(f"HTTP error: {str(e)}")
    
    async def fetch_profile(self, username: str) -> TikTokProfile:
        """
        Fetch TikTok profile data by username.
        
        Args:
            username: TikTok handle (without @)
            
        Returns:
            TikTokProfile dataclass with user info
            
        Raises:
            TikTokAPIError: If API request fails
        """
        # Remove @ if present
        username = username.lstrip("@").strip().lower()
        
        endpoint = self.endpoints["profile"]
        param_name = self.endpoints["profile_param"]
        
        data = await self._make_request(endpoint, {param_name: username})
        
        # =====================================================================
        # RESPONSE PARSING
        # Adjust based on your API's actual response structure
        # =====================================================================
        
        # Common response structures to handle:
        # 1. { "data": { "user": {...} } }
        # 2. { "userInfo": { "user": {...}, "stats": {...} } }
        # 3. { "user": {...} }
        
        try:
            # Try different response structures
            if "data" in data:
                user_data = data["data"].get("user", data["data"])
                stats_data = data["data"].get("stats", user_data)
            elif "userInfo" in data:
                user_data = data["userInfo"].get("user", {})
                stats_data = data["userInfo"].get("stats", {})
            elif "user" in data:
                user_data = data["user"]
                stats_data = data.get("stats", user_data)
            else:
                user_data = data
                stats_data = data
            
            return TikTokProfile(
                user_id=str(user_data.get("id", user_data.get("uid", ""))),
                username=user_data.get("uniqueId", user_data.get("unique_id", username)),
                display_name=user_data.get("nickname", user_data.get("nickName", "")),
                bio=user_data.get("signature", user_data.get("bio", "")),
                avatar_url=user_data.get("avatarLarger", user_data.get("avatar", "")),
                follower_count=int(stats_data.get("followerCount", stats_data.get("followers", 0))),
                following_count=int(stats_data.get("followingCount", stats_data.get("following", 0))),
                total_likes=int(stats_data.get("heartCount", stats_data.get("likes", stats_data.get("heart", 0)))),
                video_count=int(stats_data.get("videoCount", stats_data.get("videos", 0)))
            )
            
        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse profile response: {e}")
            logger.debug(f"Raw response: {data}")
            raise TikTokAPIError(f"Failed to parse profile data for @{username}")
    
    async def fetch_recent_posts(
        self,
        username: str,
        max_posts: int = 50,
        days_back: int = 30
    ) -> list[TikTokPost]:
        """
        Fetch recent posts from a TikTok profile.
        
        Args:
            username: TikTok handle (without @)
            max_posts: Maximum number of posts to fetch
            days_back: Only include posts from last N days
            
        Returns:
            List of TikTokPost dataclasses
            
        Raises:
            TikTokAPIError: If API request fails
        """
        username = username.lstrip("@").strip().lower()
        
        endpoint = self.endpoints["posts"]
        param_name = self.endpoints["posts_param"]
        
        # Request more posts than needed to filter by date
        params = {
            param_name: username,
            "count": max_posts
        }
        
        data = await self._make_request(endpoint, params)
        
        # =====================================================================
        # RESPONSE PARSING - Posts
        # =====================================================================
        
        try:
            # Try different response structures
            if "data" in data:
                posts_list = data["data"].get("videos", data["data"].get("itemList", data["data"]))
            elif "itemList" in data:
                posts_list = data["itemList"]
            elif "videos" in data:
                posts_list = data["videos"]
            else:
                posts_list = data if isinstance(data, list) else []
            
            if not isinstance(posts_list, list):
                posts_list = [posts_list] if posts_list else []
            
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)
            posts = []
            
            for item in posts_list:
                # Parse timestamp
                create_time = item.get("createTime", item.get("create_time", 0))
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
                
                # Extract stats (different API structures)
                stats = item.get("stats", item.get("statistics", item))
                
                post = TikTokPost(
                    post_id=str(item.get("id", item.get("video_id", item.get("aweme_id", "")))),
                    description=item.get("desc", item.get("description", item.get("title", ""))),
                    video_url=self._extract_video_url(item),
                    thumbnail_url=self._extract_thumbnail(item),
                    duration_seconds=int(item.get("duration", item.get("video", {}).get("duration", 0))),
                    view_count=int(stats.get("playCount", stats.get("play_count", stats.get("views", 0)))),
                    like_count=int(stats.get("diggCount", stats.get("likes", stats.get("like_count", 0)))),
                    comment_count=int(stats.get("commentCount", stats.get("comments", stats.get("comment_count", 0)))),
                    share_count=int(stats.get("shareCount", stats.get("shares", stats.get("share_count", 0)))),
                    posted_at=posted_at
                )
                
                posts.append(post)
            
            # Sort by posted date (newest first)
            posts.sort(key=lambda p: p.posted_at, reverse=True)
            
            logger.info(f"Fetched {len(posts)} posts for @{username} (last {days_back} days)")
            return posts
            
        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse posts response: {e}")
            logger.debug(f"Raw response: {data}")
            raise TikTokAPIError(f"Failed to parse posts for @{username}")
    
    def _extract_video_url(self, item: dict) -> str:
        """Extract video URL from various response formats."""
        # Try different paths
        if "video" in item:
            video = item["video"]
            return (
                video.get("playAddr", "") or
                video.get("downloadAddr", "") or
                video.get("play_addr", {}).get("url_list", [""])[0]
            )
        return item.get("video_url", item.get("play_url", ""))
    
    def _extract_thumbnail(self, item: dict) -> str:
        """Extract thumbnail URL from various response formats."""
        if "video" in item:
            video = item["video"]
            return (
                video.get("cover", "") or
                video.get("originCover", "") or
                video.get("dynamicCover", "")
            )
        return item.get("thumbnail", item.get("cover_url", ""))
    
    async def health_check(self) -> bool:
        """Test API connectivity."""
        try:
            # Try to fetch a known public profile
            await self.fetch_profile("tiktok")
            return True
        except TikTokAPIError:
            return False

