"""
Pulse - TikTok Analytics Dashboard
ScrapTik API Integration

Uses the ScrapTik API from RapidAPI to fetch TikTok post data.
API Docs: https://rapidapi.com/scraptik-api-scraptik-api-default/api/scraptik

IMPORTANT: ScrapTik requires a 2-step process for fetching user posts:
1. Convert username to user_id via /username-to-id
2. Fetch posts using user_id via /user-posts

Environment Variables Required:
    RAPIDAPI_KEY  - Your RapidAPI subscription key
    RAPIDAPI_HOST - API host (default: scraptik.p.rapidapi.com)
"""

import os
import logging
import requests
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class TikTokAPIError(Exception):
    """Custom exception for TikTok API errors."""
    pass


def get_api_headers() -> dict:
    """
    Get API headers from environment variables.
    
    Returns:
        dict: Headers for RapidAPI requests
        
    Raises:
        TikTokAPIError: If RAPIDAPI_KEY is not set
    """
    api_key = os.getenv("RAPIDAPI_KEY")
    api_host = os.getenv("RAPIDAPI_HOST", "scraptik.p.rapidapi.com")
    
    if not api_key:
        raise TikTokAPIError(
            "RAPIDAPI_KEY environment variable is not set. "
            "Please set your RapidAPI key."
        )
    
    return {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": api_host
    }


def _handle_response_errors(response: requests.Response) -> None:
    """Handle common HTTP error codes."""
    if response.status_code == 401:
        raise TikTokAPIError(
            "Invalid API key. Please check your RAPIDAPI_KEY."
        )
    
    if response.status_code == 403:
        raise TikTokAPIError(
            "Access forbidden. Your API subscription may have expired."
        )
    
    if response.status_code == 404:
        raise TikTokAPIError(
            "Endpoint not found. Check API documentation for correct endpoint."
        )
    
    if response.status_code == 429:
        raise TikTokAPIError(
            "Rate limit exceeded. Please wait before making more requests."
        )
    
    if response.status_code == 503:
        raise TikTokAPIError(
            "ScrapTik API is temporarily unavailable. Please try again later."
        )
    
    if response.status_code != 200:
        raise TikTokAPIError(
            f"API request failed with status {response.status_code}: {response.text}"
        )


# =============================================================================
# STEP 1: USERNAME TO USER ID CONVERSION
# =============================================================================

def username_to_id(username: str) -> str:
    """
    Convert a TikTok username to numeric user_id.
    
    This is REQUIRED before fetching user posts with ScrapTik API.
    
    Args:
        username: TikTok username (without @)
        
    Returns:
        str: Numeric TikTok user_id
        
    Raises:
        TikTokAPIError: If the API request fails or user not found
        
    Example:
        >>> user_id = username_to_id("charlidamelio")
        >>> print(user_id)  # "5831967"
    """
    api_host = os.getenv("RAPIDAPI_HOST", "scraptik.p.rapidapi.com")
    url = f"https://{api_host}/username-to-id"
    
    # Clean username
    username = username.lstrip("@").strip().lower()
    
    params = {
        "username": username
    }
    
    try:
        headers = get_api_headers()
        
        logger.info(f"Converting username @{username} to user_id...")
        
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        _handle_response_errors(response)
        
        data = response.json()
        
        # Check for API-level errors
        if "error" in data:
            raise TikTokAPIError(f"API error: {data['error']}")
        
        # Extract user_id from response
        # ScrapTik returns: { "user_id": "123456789" } or { "uid": "123456789" }
        user_id = data.get("user_id") or data.get("uid") or data.get("id")
        
        if not user_id:
            # Try nested structures
            if "user" in data:
                user_id = data["user"].get("id") or data["user"].get("uid")
            elif "data" in data:
                user_id = data["data"].get("user_id") or data["data"].get("uid")
        
        if not user_id:
            raise TikTokAPIError(f"Could not extract user_id for @{username}. Response: {data}")
        
        user_id = str(user_id)
        logger.info(f"✅ Converted @{username} → user_id: {user_id}")
        return user_id
        
    except requests.exceptions.Timeout:
        raise TikTokAPIError("Request timed out.")
    except requests.exceptions.ConnectionError:
        raise TikTokAPIError("Connection failed. Check your internet connection.")
    except requests.exceptions.RequestException as e:
        raise TikTokAPIError(f"Request failed: {str(e)}")


# =============================================================================
# STEP 2: FETCH USER POSTS BY USER ID
# =============================================================================

def fetch_user_posts_by_id(
    user_id: str,
    count: int = 30,
    cursor: str = "0"
) -> dict:
    """
    Fetch recent posts using the numeric user_id.
    
    This is the correct endpoint for ScrapTik - requires user_id, not username.
    
    Args:
        user_id: Numeric TikTok user ID (from username_to_id)
        count: Number of posts to fetch (default: 30, max: 35)
        cursor: Pagination cursor for fetching more posts
        
    Returns:
        dict: JSON response containing user's posts
        
    Raises:
        TikTokAPIError: If the API request fails
        
    Example:
        >>> posts = fetch_user_posts_by_id("5831967", count=30)
        >>> print(len(posts.get("aweme_list", [])))
    """
    api_host = os.getenv("RAPIDAPI_HOST", "scraptik.p.rapidapi.com")
    url = f"https://{api_host}/user-posts"
    
    params = {
        "user_id": str(user_id),
        "count": min(count, 35),  # ScrapTik max is usually 35
        "cursor": cursor
    }
    
    try:
        headers = get_api_headers()
        
        logger.info(f"Fetching {count} posts for user_id: {user_id}")
        
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        _handle_response_errors(response)
        
        data = response.json()
        
        if "error" in data:
            raise TikTokAPIError(f"API error: {data['error']}")
        
        post_count = len(data.get("aweme_list", data.get("itemList", [])))
        logger.info(f"✅ Fetched {post_count} posts for user_id: {user_id}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        raise TikTokAPIError(f"Request failed: {str(e)}")


def fetch_user_posts(
    username: str,
    count: int = 30,
    cached_user_id: Optional[str] = None
) -> Tuple[dict, str]:
    """
    Fetch user posts with automatic username-to-id conversion.
    
    This is a convenience function that handles the 2-step process:
    1. Convert username to user_id (if not cached)
    2. Fetch posts using user_id
    
    Args:
        username: TikTok username (without @)
        count: Number of posts to fetch
        cached_user_id: Optional pre-cached user_id to skip conversion
        
    Returns:
        Tuple of (posts_data, user_id) - save user_id for future calls
        
    Example:
        >>> posts, user_id = fetch_user_posts("charlidamelio")
        >>> # Save user_id to database for next time
        >>> posts2, _ = fetch_user_posts("charlidamelio", cached_user_id=user_id)
    """
    username = username.lstrip("@").strip().lower()
    
    # Use cached user_id or convert username
    if cached_user_id:
        user_id = cached_user_id
        logger.info(f"Using cached user_id for @{username}: {user_id}")
    else:
        user_id = username_to_id(username)
    
    # Fetch posts
    posts_data = fetch_user_posts_by_id(user_id, count=count)
    
    return posts_data, user_id


# =============================================================================
# FETCH SINGLE POST
# =============================================================================

def fetch_tiktok_post_data(aweme_id: str, region: str = "GB") -> dict:
    """
    Fetch TikTok post data by aweme_id (post ID).
    
    Args:
        aweme_id: The TikTok post ID (aweme_id)
        region: Region code for the request (default: "GB")
        
    Returns:
        dict: JSON response containing post data
        
    Raises:
        TikTokAPIError: If the API request fails
    """
    api_host = os.getenv("RAPIDAPI_HOST", "scraptik.p.rapidapi.com")
    url = f"https://{api_host}/get-post"
    
    params = {
        "aweme_id": aweme_id,
        "region": region
    }
    
    try:
        headers = get_api_headers()
        
        logger.info(f"Fetching TikTok post: {aweme_id}")
        
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        _handle_response_errors(response)
        
        data = response.json()
        
        if "error" in data:
            raise TikTokAPIError(f"API error: {data['error']}")
        
        logger.info(f"✅ Fetched post {aweme_id}")
        return data
        
    except requests.exceptions.RequestException as e:
        raise TikTokAPIError(f"Request failed: {str(e)}")


# =============================================================================
# FETCH USER PROFILE
# =============================================================================

def fetch_user_profile(username: str) -> dict:
    """
    Fetch TikTok user profile by username.
    
    Args:
        username: TikTok username (without @)
        
    Returns:
        dict: JSON response containing user profile data
        
    Raises:
        TikTokAPIError: If the API request fails
    """
    api_host = os.getenv("RAPIDAPI_HOST", "scraptik.p.rapidapi.com")
    url = f"https://{api_host}/get-user"
    
    username = username.lstrip("@").strip().lower()
    
    params = {
        "username": username
    }
    
    try:
        headers = get_api_headers()
        
        logger.info(f"Fetching TikTok profile: @{username}")
        
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        _handle_response_errors(response)
        
        data = response.json()
        
        if "error" in data:
            raise TikTokAPIError(f"API error: {data['error']}")
        
        logger.info(f"✅ Fetched profile @{username}")
        return data
        
    except requests.exceptions.RequestException as e:
        raise TikTokAPIError(f"Request failed: {str(e)}")


# =============================================================================
# HELPER FUNCTIONS - Extract common fields from API responses
# =============================================================================

def extract_post_stats(post_data: dict) -> dict:
    """
    Extract key statistics from a post API response.
    
    Args:
        post_data: Raw API response from fetch_tiktok_post_data()
        
    Returns:
        dict: Cleaned statistics with consistent keys
    """
    detail = post_data.get("aweme_detail", post_data)
    stats = detail.get("statistics", {})
    
    return {
        "post_id": detail.get("aweme_id", ""),
        "description": detail.get("desc", ""),
        "view_count": stats.get("play_count", 0),
        "like_count": stats.get("digg_count", 0),
        "comment_count": stats.get("comment_count", 0),
        "share_count": stats.get("share_count", 0),
        "download_count": stats.get("download_count", 0),
        "create_time": detail.get("create_time", 0),
    }


def extract_profile_stats(profile_data: dict) -> dict:
    """
    Extract key statistics from a profile API response.
    
    Args:
        profile_data: Raw API response from fetch_user_profile()
        
    Returns:
        dict: Cleaned profile data with consistent keys
    """
    user = profile_data.get("user", profile_data)
    
    # Handle nested avatar structure
    avatar_url = ""
    avatar_data = user.get("avatar_larger", user.get("avatarLarger", {}))
    if isinstance(avatar_data, dict):
        url_list = avatar_data.get("url_list", [])
        avatar_url = url_list[0] if url_list else ""
    elif isinstance(avatar_data, str):
        avatar_url = avatar_data
    
    return {
        "user_id": str(user.get("uid", user.get("id", user.get("user_id", "")))),
        "username": user.get("unique_id", user.get("uniqueId", "")),
        "display_name": user.get("nickname", ""),
        "bio": user.get("signature", ""),
        "avatar_url": avatar_url,
        "follower_count": int(user.get("follower_count", user.get("followerCount", 0))),
        "following_count": int(user.get("following_count", user.get("followingCount", 0))),
        "total_likes": int(user.get("total_favorited", user.get("heartCount", user.get("heart", 0)))),
        "video_count": int(user.get("aweme_count", user.get("videoCount", 0))),
    }


def extract_posts_from_response(posts_data: dict) -> list[dict]:
    """
    Extract and normalize posts from API response.
    
    Args:
        posts_data: Raw API response from fetch_user_posts_by_id()
        
    Returns:
        list: List of normalized post dictionaries
    """
    # Try different response structures
    posts_list = (
        posts_data.get("aweme_list") or
        posts_data.get("itemList") or
        posts_data.get("videos") or
        posts_data.get("data", {}).get("aweme_list") or
        []
    )
    
    normalized = []
    for post in posts_list:
        stats = post.get("statistics", post.get("stats", {}))
        
        # Parse create_time
        create_time = post.get("create_time", post.get("createTime", 0))
        
        normalized.append({
            "post_id": str(post.get("aweme_id", post.get("id", ""))),
            "description": post.get("desc", post.get("description", "")),
            "view_count": int(stats.get("play_count", stats.get("playCount", 0))),
            "like_count": int(stats.get("digg_count", stats.get("diggCount", 0))),
            "comment_count": int(stats.get("comment_count", stats.get("commentCount", 0))),
            "share_count": int(stats.get("share_count", stats.get("shareCount", 0))),
            "create_time": create_time,
            "duration": post.get("duration", 0),
            "video_url": _extract_video_url(post),
            "thumbnail_url": _extract_thumbnail(post),
        })
    
    return normalized


def _extract_video_url(post: dict) -> str:
    """Extract video URL from post data."""
    video = post.get("video", {})
    if isinstance(video, dict):
        play_addr = video.get("play_addr", {})
        if isinstance(play_addr, dict):
            url_list = play_addr.get("url_list", [])
            if url_list:
                return url_list[0]
        return video.get("playAddr", "")
    return ""


def _extract_thumbnail(post: dict) -> str:
    """Extract thumbnail URL from post data."""
    video = post.get("video", {})
    if isinstance(video, dict):
        cover = video.get("cover", video.get("origin_cover", {}))
        if isinstance(cover, dict):
            url_list = cover.get("url_list", [])
            if url_list:
                return url_list[0]
        elif isinstance(cover, str):
            return cover
    return ""


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    """Test the API functions from command line."""
    import sys
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  python tiktok_api.py convert <username>  - Get user_id from username")
        print("  python tiktok_api.py posts <username>    - Fetch posts (2-step)")
        print("  python tiktok_api.py posts-id <user_id>  - Fetch posts by user_id")
        print("  python tiktok_api.py profile <username>  - Fetch profile")
        print("  python tiktok_api.py post <aweme_id>     - Fetch single post")
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == "convert" and len(sys.argv) >= 3:
            username = sys.argv[2]
            user_id = username_to_id(username)
            print(f"\n✅ @{username} → user_id: {user_id}")
            
        elif command == "posts" and len(sys.argv) >= 3:
            username = sys.argv[2]
            print(f"\n📥 Fetching posts for @{username} (2-step process)...")
            posts_data, user_id = fetch_user_posts(username, count=30)
            posts = extract_posts_from_response(posts_data)
            
            print(f"\n✅ user_id: {user_id}")
            print(f"📹 Found {len(posts)} posts:\n")
            
            for i, post in enumerate(posts[:5], 1):
                print(f"   {i}. {post['view_count']:,} views | {post['description'][:50]}...")
            
        elif command == "posts-id" and len(sys.argv) >= 3:
            user_id = sys.argv[2]
            posts_data = fetch_user_posts_by_id(user_id, count=30)
            posts = extract_posts_from_response(posts_data)
            
            print(f"\n📹 Found {len(posts)} posts for user_id {user_id}:\n")
            for i, post in enumerate(posts[:5], 1):
                print(f"   {i}. {post['view_count']:,} views")
            
        elif command == "profile" and len(sys.argv) >= 3:
            username = sys.argv[2]
            data = fetch_user_profile(username)
            stats = extract_profile_stats(data)
            
            print(f"\n👤 Profile: @{stats['username']}")
            print(f"   user_id: {stats['user_id']}")
            print(f"   Name: {stats['display_name']}")
            print(f"   Followers: {stats['follower_count']:,}")
            print(f"   Following: {stats['following_count']:,}")
            print(f"   Likes: {stats['total_likes']:,}")
            print(f"   Videos: {stats['video_count']}")
            
        elif command == "post" and len(sys.argv) >= 3:
            aweme_id = sys.argv[2]
            data = fetch_tiktok_post_data(aweme_id)
            stats = extract_post_stats(data)
            
            print(f"\n📊 Post Stats for {aweme_id}:")
            print(f"   Views: {stats['view_count']:,}")
            print(f"   Likes: {stats['like_count']:,}")
            print(f"   Comments: {stats['comment_count']:,}")
            print(f"   Shares: {stats['share_count']:,}")
            
        else:
            print(f"Unknown command: {command}")
            
    except TikTokAPIError as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
