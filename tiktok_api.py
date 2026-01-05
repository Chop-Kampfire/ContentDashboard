"""
Pulse - TikTok Analytics Dashboard
ScrapTik API Integration

Uses the ScrapTik API from RapidAPI to fetch TikTok post data.
API Docs: https://rapidapi.com/scraptik-api-scraptik-api-default/api/scraptik

Environment Variables Required:
    RAPIDAPI_KEY  - Your RapidAPI subscription key
    RAPIDAPI_HOST - API host (default: scraptik.p.rapidapi.com)
"""

import os
import logging
import requests
from typing import Optional

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


def fetch_tiktok_post_data(aweme_id: str, region: str = "GB") -> dict:
    """
    Fetch TikTok post data by aweme_id (post ID).
    
    Args:
        aweme_id: The TikTok post ID (aweme_id)
        region: Region code for the request (default: "GB")
        
    Returns:
        dict: JSON response from the ScrapTik API containing post data
        
    Raises:
        TikTokAPIError: If the API request fails
        
    Example:
        >>> data = fetch_tiktok_post_data("7493156277590428974")
        >>> print(data["aweme_detail"]["statistics"]["play_count"])
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
        
        # Handle HTTP errors
        if response.status_code == 401:
            raise TikTokAPIError(
                "Invalid API key. Please check your RAPIDAPI_KEY."
            )
        
        if response.status_code == 403:
            raise TikTokAPIError(
                "Access forbidden. Your API subscription may have expired."
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
        
        data = response.json()
        
        # Check for API-level errors in response
        if "error" in data:
            raise TikTokAPIError(f"API error: {data['error']}")
        
        logger.info(f"Successfully fetched post {aweme_id}")
        return data
        
    except requests.exceptions.Timeout:
        raise TikTokAPIError(
            "Request timed out. The API server may be slow or unavailable."
        )
    except requests.exceptions.ConnectionError:
        raise TikTokAPIError(
            "Connection failed. Please check your internet connection."
        )
    except requests.exceptions.RequestException as e:
        raise TikTokAPIError(f"Request failed: {str(e)}")


def fetch_user_profile(username: str, region: str = "GB") -> dict:
    """
    Fetch TikTok user profile by username.
    
    Args:
        username: TikTok username (without @)
        region: Region code for the request (default: "GB")
        
    Returns:
        dict: JSON response containing user profile data
        
    Raises:
        TikTokAPIError: If the API request fails
    """
    api_host = os.getenv("RAPIDAPI_HOST", "scraptik.p.rapidapi.com")
    url = f"https://{api_host}/get-user"
    
    # Remove @ if present
    username = username.lstrip("@").strip()
    
    params = {
        "username": username,
        "region": region
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
        
        if response.status_code == 401:
            raise TikTokAPIError("Invalid API key.")
        
        if response.status_code == 429:
            raise TikTokAPIError("Rate limit exceeded.")
        
        if response.status_code != 200:
            raise TikTokAPIError(
                f"API request failed with status {response.status_code}"
            )
        
        data = response.json()
        logger.info(f"Successfully fetched profile @{username}")
        return data
        
    except requests.exceptions.RequestException as e:
        raise TikTokAPIError(f"Request failed: {str(e)}")


def fetch_user_posts(username: str, count: int = 30, region: str = "GB") -> dict:
    """
    Fetch recent posts from a TikTok user.
    
    Args:
        username: TikTok username (without @)
        count: Number of posts to fetch (default: 30)
        region: Region code for the request (default: "GB")
        
    Returns:
        dict: JSON response containing user's posts
        
    Raises:
        TikTokAPIError: If the API request fails
    """
    api_host = os.getenv("RAPIDAPI_HOST", "scraptik.p.rapidapi.com")
    url = f"https://{api_host}/get-user-posts"
    
    username = username.lstrip("@").strip()
    
    params = {
        "username": username,
        "count": count,
        "region": region
    }
    
    try:
        headers = get_api_headers()
        
        logger.info(f"Fetching posts for @{username}")
        
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        if response.status_code != 200:
            raise TikTokAPIError(
                f"API request failed with status {response.status_code}"
            )
        
        data = response.json()
        logger.info(f"Successfully fetched {count} posts for @{username}")
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
    # Navigate to the aweme_detail object
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
    
    return {
        "user_id": user.get("uid", user.get("id", "")),
        "username": user.get("unique_id", user.get("uniqueId", "")),
        "display_name": user.get("nickname", ""),
        "bio": user.get("signature", ""),
        "avatar_url": user.get("avatar_larger", {}).get("url_list", [""])[0],
        "follower_count": user.get("follower_count", 0),
        "following_count": user.get("following_count", 0),
        "total_likes": user.get("total_favorited", 0),
        "video_count": user.get("aweme_count", 0),
    }


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    """Test the API functions from command line."""
    import sys
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    load_dotenv()
    
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python tiktok_api.py post <aweme_id>")
        print("  python tiktok_api.py profile <username>")
        print("  python tiktok_api.py posts <username>")
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == "post" and len(sys.argv) >= 3:
            aweme_id = sys.argv[2]
            data = fetch_tiktok_post_data(aweme_id)
            stats = extract_post_stats(data)
            print(f"\n📊 Post Stats for {aweme_id}:")
            print(f"   Views: {stats['view_count']:,}")
            print(f"   Likes: {stats['like_count']:,}")
            print(f"   Comments: {stats['comment_count']:,}")
            print(f"   Shares: {stats['share_count']:,}")
            
        elif command == "profile" and len(sys.argv) >= 3:
            username = sys.argv[2]
            data = fetch_user_profile(username)
            stats = extract_profile_stats(data)
            print(f"\n👤 Profile: @{stats['username']}")
            print(f"   Name: {stats['display_name']}")
            print(f"   Followers: {stats['follower_count']:,}")
            print(f"   Following: {stats['following_count']:,}")
            print(f"   Likes: {stats['total_likes']:,}")
            print(f"   Videos: {stats['video_count']}")
            
        elif command == "posts" and len(sys.argv) >= 3:
            username = sys.argv[2]
            data = fetch_user_posts(username)
            posts = data.get("aweme_list", [])
            print(f"\n📹 Recent posts for @{username}: {len(posts)} found")
            for i, post in enumerate(posts[:5], 1):
                stats = post.get("statistics", {})
                print(f"   {i}. {stats.get('play_count', 0):,} views")
                
        else:
            print(f"Unknown command: {command}")
            
    except TikTokAPIError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

