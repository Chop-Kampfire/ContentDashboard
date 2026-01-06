"""
Pulse - Multi-Platform Analytics Dashboard
Main Streamlit Application
Version 0.0.3

A sleek, investment-style analytics dashboard for tracking TikTok performance.
Automatic database migrations on Railway deployment.
"""

import os
import asyncio
from datetime import datetime, timedelta
from typing import Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, func, desc, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError

from database.models import Base, Profile, ProfileHistory, Post, PostHistory, AlertLog
from services.logger import get_logger, setup_root_logger

# Initialize logging
setup_root_logger()
logger = get_logger(__name__)

# =============================================================================
# PAGE CONFIG & STYLING
# =============================================================================

st.set_page_config(
    page_title="Pulse • Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a distinctive, modern aesthetic
st.markdown("""
<style>
    /* Import distinctive fonts */
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* Root variables - Dark mode with electric accents */
    :root {
        --bg-primary: #0a0a0f;
        --bg-secondary: #12121a;
        --bg-card: #1a1a24;
        --accent-primary: #00d4aa;
        --accent-secondary: #ff6b6b;
        --accent-tertiary: #ffd93d;
        --text-primary: #ffffff;
        --text-secondary: #a0a0b0;
        --border-color: #2a2a3a;
    }
    
    /* Main container */
    .main .block-container {
        padding: 2rem 3rem;
        max-width: 1400px;
    }
    
    /* Typography */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 600 !important;
    }
    
    p, span, div, label {
        font-family: 'Outfit', sans-serif !important;
    }
    
    code {
        font-family: 'JetBrains Mono', monospace !important;
    }
    
    /* Header styling */
    .pulse-header {
        background: linear-gradient(135deg, #1a1a24 0%, #0a0a0f 100%);
        border: 1px solid var(--border-color);
        border-radius: 16px;
        padding: 2rem;
        margin-bottom: 2rem;
    }
    
    .pulse-title {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #00d4aa 0%, #00a896 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    
    .pulse-subtitle {
        color: var(--text-secondary);
        font-size: 1.1rem;
        font-weight: 400;
    }
    
    /* Metric cards */
    .metric-card {
        background: linear-gradient(145deg, #1a1a24 0%, #12121a 100%);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 32px rgba(0, 212, 170, 0.1);
    }
    
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: var(--text-primary);
        font-family: 'JetBrains Mono', monospace !important;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 0.5rem;
    }
    
    .metric-delta-positive {
        color: #00d4aa;
        font-size: 0.95rem;
    }
    
    .metric-delta-negative {
        color: #ff6b6b;
        font-size: 0.95rem;
    }
    
    /* Viral badge */
    .viral-badge {
        background: linear-gradient(135deg, #ff6b6b 0%, #ff8e53 100%);
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: transparent;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #00d4aa 0%, #00a896 100%);
        border-color: transparent;
    }
    
    /* Dataframe styling */
    .dataframe {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.85rem;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(135deg, #00d4aa 0%, #00a896 100%);
        color: #0a0a0f;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        font-family: 'Outfit', sans-serif;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 16px rgba(0, 212, 170, 0.3);
    }
    
    /* Input styling */
    .stTextInput > div > div > input {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        color: var(--text-primary);
        font-family: 'Outfit', sans-serif;
    }
    
    /* Selectbox styling */
    .stSelectbox > div > div {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 8px;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background: var(--bg-secondary);
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Chart container */
    .chart-container {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 1rem;
    }
    
    /* Error box */
    .schema-error {
        background: linear-gradient(135deg, #2a1a1a 0%, #1a0a0a 100%);
        border: 2px solid #ff6b6b;
        border-radius: 12px;
        padding: 2rem;
        margin: 2rem 0;
    }
    
    .schema-error h3 {
        color: #ff6b6b;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

@st.cache_resource
def get_database_engine():
    """Create and cache database engine."""
    database_url = os.getenv("DATABASE_URL", "")
    
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        st.error("⚠️ DATABASE_URL environment variable not set")
        return None
    
    # Fix postgres:// to postgresql:// for SQLAlchemy 2.0+
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    sql_echo = os.getenv("SQL_ECHO", "false").lower() == "true"
    
    try:
        engine = create_engine(database_url, pool_pre_ping=True, echo=sql_echo)
        Base.metadata.create_all(bind=engine)
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        st.error(f"⚠️ Database connection failed: {e}")
        return None


def get_session():
    """Get a database session."""
    engine = get_database_engine()
    if engine:
        Session = sessionmaker(bind=engine)
        return Session()
    return None


# =============================================================================
# SCHEMA VALIDATION (STARTUP SANITY CHECK)
# =============================================================================

def check_schema_health() -> dict:
    """
    Check if required columns exist in the database.
    Returns dict with 'healthy' bool and 'missing_columns' list.
    """
    engine = get_database_engine()
    if not engine:
        return {"healthy": False, "error": "No database connection", "missing_columns": []}
    
    result = {
        "healthy": True,
        "missing_columns": [],
        "error": None,
        "schema_version": "unknown"
    }
    
    # Required columns for v0.0.2
    required_columns = {
        "profiles": ["platform", "platform_user_id", "user_role"],
        "posts": ["platform", "upvote_ratio", "is_crosspost", "retweet_count", "quote_count"]
    }
    
    try:
        with engine.connect() as conn:
            # Check schema version
            check_platform = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'profiles' AND column_name = 'platform'
            """))
            
            if check_platform.fetchone():
                result["schema_version"] = "0.0.2"
            else:
                result["schema_version"] = "0.0.1"
            
            # Check all required columns
            for table, columns in required_columns.items():
                for column in columns:
                    check = conn.execute(text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table AND column_name = :column
                    """), {"table": table, "column": column})
                    
                    if check.fetchone() is None:
                        result["missing_columns"].append(f"{table}.{column}")
                        result["healthy"] = False
        
        if result["missing_columns"]:
            logger.error(f"Schema check FAILED - missing columns: {result['missing_columns']}")
        else:
            logger.info(f"Schema check PASSED - version {result['schema_version']}")
            
    except (OperationalError, ProgrammingError) as e:
        result["healthy"] = False
        result["error"] = str(e)
        logger.error(f"Schema check ERROR: {e}")
    
    return result


def display_schema_error(health: dict):
    """Display a user-friendly schema error message."""
    st.markdown("""
    <div class="schema-error">
        <h3>⚠️ Database Schema Out of Date</h3>
        <p>The database is missing required columns for schema version 0.0.2.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.error("**Migration Required**")
    
    with st.expander("📋 Missing Columns", expanded=True):
        for col in health.get("missing_columns", []):
            st.code(col)
    
    st.markdown("### How to Fix")
    st.markdown("""
    Run the migration script using Railway CLI:
    
    ```bash
    railway run python migrate_v002.py
    ```
    
    Or run it locally with your DATABASE_URL set:
    
    ```bash
    export DATABASE_URL="your-connection-string"
    python migrate_v002.py
    ```
    """)
    
    st.info("After running the migration, refresh this page.")
    
    if health.get("error"):
        with st.expander("🔧 Technical Details"):
            st.code(health["error"])


# =============================================================================
# DATA FETCHING FUNCTIONS
# =============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_all_profiles() -> pd.DataFrame:
    """Fetch all active profiles."""
    session = get_session()
    if not session:
        return pd.DataFrame()
    
    try:
        profiles = session.query(Profile).filter(Profile.is_active == True).all()
        
        data = [{
            "id": p.id,
            "platform": p.platform or 'tiktok',
            "username": p.username,
            "display_name": p.display_name or p.username,
            "avatar_url": p.avatar_url,
            "followers": p.follower_count,
            "total_likes": p.total_likes,
            "videos": p.video_count,
            "avg_views": p.average_post_views,
            "last_updated": p.last_scraped_at
        } for p in profiles]
        
        logger.debug(f"Fetched {len(data)} active profiles")
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error fetching profiles: {e}")
        return pd.DataFrame()
    finally:
        session.close()


@st.cache_data(ttl=300)
def get_profile_history(profile_id: int, days: int = 30) -> pd.DataFrame:
    """Fetch profile history for charting."""
    session = get_session()
    if not session:
        return pd.DataFrame()
    
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        history = session.query(ProfileHistory).filter(
            ProfileHistory.profile_id == profile_id,
            ProfileHistory.recorded_at >= cutoff
        ).order_by(ProfileHistory.recorded_at).all()
        
        data = [{
            "date": h.recorded_at,
            "followers": h.follower_count,
            "likes": h.total_likes,
            "videos": h.video_count,
            "follower_change": h.follower_change
        } for h in history]
        
        return pd.DataFrame(data)
    finally:
        session.close()


@st.cache_data(ttl=300)
def get_all_posts(days: int = 30) -> pd.DataFrame:
    """Fetch all posts from the last N days."""
    session = get_session()
    if not session:
        return pd.DataFrame()
    
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        posts = session.query(Post, Profile).join(
            Profile, Post.profile_id == Profile.id
        ).filter(
            Post.posted_at >= cutoff
        ).order_by(desc(Post.view_count)).all()

        data = []
        for p in posts:
            platform = p.Profile.platform or 'tiktok'

            # Platform-specific efficacy metric
            if platform == 'reddit':
                efficacy_metric = p.Post.reddit_score or 0
                efficacy_metric_name = 'Score'
                avg_metric = p.Profile.average_post_views  # Could be renamed to average_post_score in future
            else:
                efficacy_metric = p.Post.view_count
                efficacy_metric_name = 'Views'
                avg_metric = p.Profile.average_post_views

            # Calculate efficacy percentage
            efficacy_score = round((efficacy_metric / avg_metric * 100), 1) if avg_metric > 0 else 0

            data.append({
                "id": p.Post.id,
                "post_id": p.Post.platform_post_id or p.Post.tiktok_post_id,
                "platform": platform,
                "username": f"@{p.Profile.username}",
                "description": (p.Post.description or "")[:80] + "..." if p.Post.description and len(p.Post.description) > 80 else (p.Post.description or ""),
                "efficacy_metric": efficacy_metric,
                "efficacy_metric_name": efficacy_metric_name,
                "views": p.Post.view_count,
                "likes": p.Post.like_count,
                "comments": p.Post.comment_count,
                "shares": p.Post.share_count,
                "reddit_score": p.Post.reddit_score or 0,
                "retweets": p.Post.retweet_count or 0,
                "is_viral": p.Post.is_viral,
                "posted_at": p.Post.posted_at,
                "avg_metric": avg_metric,
                "efficacy_score": efficacy_score
            })

        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error fetching posts: {e}")
        return pd.DataFrame()
    finally:
        session.close()


@st.cache_data(ttl=300)
def get_aggregate_stats() -> dict:
    """Get aggregate statistics across all profiles."""
    session = get_session()
    if not session:
        return {}
    
    try:
        # Total followers
        total_followers = session.query(func.sum(Profile.follower_count)).filter(
            Profile.is_active == True
        ).scalar() or 0
        
        # Total profiles
        total_profiles = session.query(func.count(Profile.id)).filter(
            Profile.is_active == True
        ).scalar() or 0
        
        # Total posts tracked
        total_posts = session.query(func.count(Post.id)).scalar() or 0
        
        # Total views (last 30 days)
        cutoff = datetime.utcnow() - timedelta(days=30)
        total_views = session.query(func.sum(Post.view_count)).filter(
            Post.posted_at >= cutoff
        ).scalar() or 0
        
        # Viral posts count
        viral_count = session.query(func.count(Post.id)).filter(
            Post.is_viral == True
        ).scalar() or 0
        
        # Follower growth (24h)
        yesterday = datetime.utcnow() - timedelta(hours=24)
        growth_data = session.query(func.sum(ProfileHistory.follower_change)).filter(
            ProfileHistory.recorded_at >= yesterday
        ).scalar() or 0
        
        return {
            "total_followers": total_followers,
            "total_profiles": total_profiles,
            "total_posts": total_posts,
            "total_views": total_views,
            "viral_posts": viral_count,
            "follower_growth_24h": growth_data
        }
    finally:
        session.close()


# =============================================================================
# CHART FUNCTIONS
# =============================================================================

def create_follower_growth_chart(profile_ids: list[int], profile_names: dict) -> go.Figure:
    """Create investment-style follower growth line chart."""
    
    fig = go.Figure()
    
    colors = ['#00d4aa', '#ff6b6b', '#ffd93d', '#a78bfa', '#60a5fa', '#f472b6']
    
    for i, profile_id in enumerate(profile_ids):
        df = get_profile_history(profile_id, days=30)
        if df.empty:
            continue
        
        color = colors[i % len(colors)]
        name = profile_names.get(profile_id, f"Profile {profile_id}")
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['followers'],
            mode='lines',
            name=f"@{name}",
            line=dict(color=color, width=2.5),
            fill='tonexty' if i > 0 else 'tozeroy',
            fillcolor=f"rgba{tuple(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.1,)}"
        ))
    
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Outfit, sans-serif", color="#a0a0b0"),
        title=dict(
            text="Follower Growth Trend",
            font=dict(size=18, color="#ffffff"),
            x=0
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.05)',
            linecolor='rgba(255,255,255,0.1)',
            title=""
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.05)',
            linecolor='rgba(255,255,255,0.1)',
            title="Followers",
            tickformat=",d"
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor='rgba(0,0,0,0)'
        ),
        hovermode="x unified",
        margin=dict(l=0, r=0, t=60, b=0),
        height=400
    )
    
    return fig


def create_post_performance_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart comparing post views vs average."""
    
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig
    
    # Get top 10 posts by views
    top_posts = df.nlargest(10, 'views')
    
    fig = go.Figure()
    
    # Actual views bars
    fig.add_trace(go.Bar(
        x=top_posts['username'],
        y=top_posts['views'],
        name='Post Views',
        marker_color='#00d4aa',
        marker_line_color='#00a896',
        marker_line_width=1
    ))
    
    # Average line
    fig.add_trace(go.Scatter(
        x=top_posts['username'],
        y=top_posts['avg_views'],
        name='30-Day Average',
        mode='lines+markers',
        line=dict(color='#ff6b6b', width=2, dash='dash'),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Outfit, sans-serif", color="#a0a0b0"),
        title=dict(
            text="Top Posts vs. Account Average",
            font=dict(size=18, color="#ffffff"),
            x=0
        ),
        xaxis=dict(
            showgrid=False,
            linecolor='rgba(255,255,255,0.1)',
            title=""
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.05)',
            linecolor='rgba(255,255,255,0.1)',
            title="Views",
            tickformat=",d"
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor='rgba(0,0,0,0)'
        ),
        barmode='group',
        margin=dict(l=0, r=0, t=60, b=0),
        height=400
    )
    
    return fig


def create_efficacy_gauge(score: float) -> go.Figure:
    """Create a gauge chart for efficacy score."""
    
    color = '#00d4aa' if score >= 100 else '#ffd93d' if score >= 50 else '#ff6b6b'
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        number={'suffix': '%', 'font': {'size': 40, 'family': 'JetBrains Mono'}},
        delta={'reference': 100, 'relative': False},
        gauge={
            'axis': {'range': [0, 300], 'tickcolor': '#a0a0b0'},
            'bar': {'color': color},
            'bgcolor': '#1a1a24',
            'borderwidth': 0,
            'steps': [
                {'range': [0, 50], 'color': 'rgba(255,107,107,0.2)'},
                {'range': [50, 100], 'color': 'rgba(255,217,61,0.2)'},
                {'range': [100, 300], 'color': 'rgba(0,212,170,0.2)'}
            ],
            'threshold': {
                'line': {'color': '#ffffff', 'width': 2},
                'thickness': 0.75,
                'value': 100
            }
        }
    ))
    
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Outfit, sans-serif", color="#a0a0b0"),
        margin=dict(l=20, r=20, t=20, b=20),
        height=200
    )
    
    return fig


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def format_number(n: int) -> str:
    """Format large numbers with K/M suffix."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def add_profile_to_watchlist(username: str, platform: str = 'tiktok') -> tuple[bool, str]:
    """
    Add a new profile to the watchlist.

    Args:
        username: Platform handle (without @)
        platform: Platform type ('tiktok', 'twitter', 'reddit'). Defaults to 'tiktok'.

    Returns:
        Tuple of (success: bool, message: str)
    """
    from scraper import ScraperFactory

    logger.info(f"Adding {platform} profile to watchlist: @{username}")

    try:
        scraper = ScraperFactory.get_scraper(platform)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        profile = loop.run_until_complete(scraper.add_profile(username, send_notification=True))
        loop.close()

        # Clear cache to refresh data
        get_all_profiles.clear()
        get_aggregate_stats.clear()
        get_all_posts.clear()

        logger.info(f"Successfully added {platform} profile: @{profile.username}")
        return True, f"Successfully added @{profile.username} ({platform})"
    except NotImplementedError as e:
        logger.warning(f"{platform.title()} scraper not implemented: {e}")
        return False, f"{platform.title()} integration coming soon! Currently only TikTok is supported."
    except Exception as e:
        logger.error(f"Failed to add profile @{username} ({platform}): {e}")
        return False, f"Error: {str(e)}"


def remove_profile_from_watchlist(username: str) -> tuple[bool, str]:
    """Remove a profile from the watchlist."""
    session = get_session()
    if not session:
        return False, "Database connection failed"
    
    try:
        profile = session.query(Profile).filter(Profile.username == username.lower()).first()
        if profile:
            profile.is_active = False
            session.commit()
            
            # Clear cache
            get_all_profiles.clear()
            get_aggregate_stats.clear()
            
            logger.info(f"Removed profile from watchlist: @{username}")
            return True, f"Removed @{username} from watchlist"
        return False, f"Profile @{username} not found"
    finally:
        session.close()


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    """Main application entry point."""
    
    # Header
    st.markdown("""
    <div class="pulse-header">
        <div class="pulse-title">📊 Pulse</div>
        <div class="pulse-subtitle">Analytics Dashboard • Real-time performance tracking</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Check database connection
    engine = get_database_engine()
    if not engine:
        st.warning("⚠️ Please configure DATABASE_URL environment variable to connect to PostgreSQL.")
        st.code("DATABASE_URL=postgresql://user:password@host:port/database")
        return
    
    # SCHEMA SANITY CHECK
    schema_health = check_schema_health()
    if not schema_health["healthy"]:
        display_schema_error(schema_health)
        return
    
    # Fetch data
    profiles_df = get_all_profiles()
    posts_df = get_all_posts(days=30)
    stats = get_aggregate_stats()
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📈 Overview", "🎬 Post Performance", "⚙️ Watchlist Management"])
    
    # =========================================================================
    # TAB 1: OVERVIEW
    # =========================================================================
    with tab1:
        st.markdown("### Performance Snapshot")
        
        # Metric cards
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                label="Tracked Profiles",
                value=stats.get('total_profiles', 0),
                delta=None
            )
        
        with col2:
            st.metric(
                label="Total Followers",
                value=format_number(stats.get('total_followers', 0)),
                delta=f"+{format_number(stats.get('follower_growth_24h', 0))} (24h)" if stats.get('follower_growth_24h', 0) > 0 else None
            )
        
        with col3:
            st.metric(
                label="Total Views (30d)",
                value=format_number(stats.get('total_views', 0)),
                delta=None
            )
        
        with col4:
            st.metric(
                label="Posts Tracked",
                value=format_number(stats.get('total_posts', 0)),
                delta=None
            )
        
        with col5:
            st.metric(
                label="Viral Posts 🔥",
                value=stats.get('viral_posts', 0),
                delta=None
            )
        
        st.markdown("---")
        
        # Charts section
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.markdown("#### Follower Growth")
            
            if not profiles_df.empty:
                # Profile selector for chart
                selected_profiles = st.multiselect(
                    "Select profiles to compare",
                    options=profiles_df['id'].tolist(),
                    format_func=lambda x: f"@{profiles_df[profiles_df['id'] == x]['username'].values[0]}",
                    default=profiles_df['id'].tolist()[:3]  # Default to first 3
                )
                
                if selected_profiles:
                    profile_names = dict(zip(profiles_df['id'], profiles_df['username']))
                    fig = create_follower_growth_chart(selected_profiles, profile_names)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Select profiles to view growth chart")
            else:
                st.info("No profiles tracked yet. Add profiles in the Watchlist tab.")
        
        with col_right:
            st.markdown("#### Post Performance vs. Average")
            
            if not posts_df.empty:
                fig = create_post_performance_chart(posts_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No posts data available yet.")
        
        # Profile cards
        st.markdown("---")
        st.markdown("#### Tracked Profiles")
        
        if not profiles_df.empty:
            cols = st.columns(min(4, len(profiles_df)))
            
            for i, (_, profile) in enumerate(profiles_df.head(8).iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div style="font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;">
                            @{profile['username']}
                        </div>
                        <div class="metric-value">{format_number(profile['followers'])}</div>
                        <div class="metric-label">Followers</div>
                        <div style="margin-top: 0.5rem; font-size: 0.85rem; color: var(--text-secondary);">
                            {profile['videos']} videos • {format_number(int(profile['avg_views']))} avg views
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("No profiles tracked yet. Add profiles in the Watchlist Management tab.")
    
    # =========================================================================
    # TAB 2: POST PERFORMANCE
    # =========================================================================
    with tab2:
        st.markdown("### Post Analytics")
        
        if posts_df.empty:
            st.info("No posts data available. Add profiles and wait for the scraper to fetch data.")
        else:
            # Filters
            col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

            with col1:
                search_term = st.text_input("🔍 Search posts", placeholder="Search by username or description...")

            with col2:
                # Platform filter
                platforms_in_data = ['All'] + sorted(posts_df['platform'].unique().tolist())
                platform_filter = st.selectbox("Platform", platforms_in_data)

            with col3:
                viral_filter = st.selectbox("Filter", ["All Posts", "Viral Only 🔥", "Below Average"])

            with col4:
                sort_by = st.selectbox("Sort by", ["Performance (High to Low)", "Efficacy Score", "Most Recent"])

            # Apply filters
            filtered_df = posts_df.copy()

            if search_term:
                filtered_df = filtered_df[
                    filtered_df['username'].str.contains(search_term, case=False) |
                    filtered_df['description'].str.contains(search_term, case=False, na=False)
                ]

            if platform_filter and platform_filter != 'All':
                filtered_df = filtered_df[filtered_df['platform'] == platform_filter]

            if viral_filter == "Viral Only 🔥":
                filtered_df = filtered_df[filtered_df['is_viral'] == True]
            elif viral_filter == "Below Average":
                filtered_df = filtered_df[filtered_df['efficacy_score'] < 100]

            # Sort (platform-aware)
            if sort_by == "Performance (High to Low)":
                # Sort by platform-specific efficacy metric (Views or Score)
                filtered_df = filtered_df.sort_values('efficacy_metric', ascending=False)
            elif sort_by == "Efficacy Score":
                filtered_df = filtered_df.sort_values('efficacy_score', ascending=False)
            elif sort_by == "Most Recent":
                filtered_df = filtered_df.sort_values('posted_at', ascending=False)
            
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                avg_efficacy = filtered_df['efficacy_score'].mean() if not filtered_df.empty else 0
                st.metric("Avg Efficacy Score", f"{avg_efficacy:.1f}%")
            with col2:
                total_views = filtered_df['views'].sum() if not filtered_df.empty else 0
                st.metric("Total Views", format_number(total_views))
            with col3:
                viral_count = filtered_df['is_viral'].sum() if not filtered_df.empty else 0
                st.metric("Viral Posts", viral_count)
            
            st.markdown("---")

            # Display table (platform-aware columns)
            display_cols = ['platform', 'username', 'description', 'efficacy_metric', 'likes', 'comments', 'shares', 'efficacy_score', 'is_viral', 'posted_at']

            # Format for display
            display_df = filtered_df[display_cols].copy()
            display_df.columns = ['Platform', 'Account', 'Description', 'Performance', 'Likes', 'Comments', 'Shares', 'Efficacy %', 'Viral 🔥', 'Posted']

            # Platform icon/emoji
            platform_icons = {'tiktok': '🎵', 'twitter': '🐦', 'reddit': '🔴'}
            display_df['Platform'] = display_df['Platform'].apply(lambda x: f"{platform_icons.get(x, '📱')} {x.title()}")

            # Format numbers - Performance is platform-specific (Views for TikTok/Twitter, Score for Reddit)
            display_df['Performance'] = display_df['Performance'].apply(lambda x: f"{x:,}")
            display_df['Likes'] = display_df['Likes'].apply(lambda x: f"{x:,}")
            display_df['Efficacy %'] = display_df['Efficacy %'].apply(lambda x: f"{x:.1f}%")
            display_df['Viral 🔥'] = display_df['Viral 🔥'].apply(lambda x: "🔥" if x else "")
            display_df['Posted'] = pd.to_datetime(display_df['Posted']).dt.strftime('%Y-%m-%d %H:%M')

            st.dataframe(
                display_df,
                use_container_width=True,
                height=500,
                hide_index=True
            )
            
            # Post detail view
            st.markdown("---")
            st.markdown("#### Post Detail View")
            
            if not filtered_df.empty:
                selected_post = st.selectbox(
                    "Select a post to analyze",
                    options=filtered_df['post_id'].tolist(),
                    format_func=lambda x: f"{filtered_df[filtered_df['post_id'] == x]['username'].values[0]} - {filtered_df[filtered_df['post_id'] == x]['description'].values[0][:50]}..."
                )
                
                if selected_post:
                    post_data = filtered_df[filtered_df['post_id'] == selected_post].iloc[0]
                    
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        st.markdown("##### Efficacy Score")
                        fig = create_efficacy_gauge(post_data['efficacy_score'])
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        st.markdown("##### Engagement Breakdown")

                        # Platform-specific metrics
                        platform = post_data.get('platform', 'tiktok')
                        if platform == 'reddit':
                            metric_label = 'Score'
                            metric_value = post_data.get('reddit_score', 0)
                        else:
                            metric_label = 'Views'
                            metric_value = post_data.get('views', 0)

                        engagement_data = {
                            'Metric': [metric_label, 'Likes', 'Comments', 'Shares'],
                            'Count': [metric_value, post_data['likes'], post_data['comments'], post_data['shares']]
                        }
                        fig = px.bar(
                            engagement_data,
                            x='Metric',
                            y='Count',
                            color='Metric',
                            color_discrete_sequence=['#00d4aa', '#ff6b6b', '#ffd93d', '#a78bfa']
                        )
                        fig.update_layout(
                            template="plotly_dark",
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            showlegend=False,
                            margin=dict(l=0, r=0, t=20, b=0),
                            height=250
                        )
                        st.plotly_chart(fig, use_container_width=True)
    
    # =========================================================================
    # TAB 3: WATCHLIST MANAGEMENT
    # =========================================================================
    with tab3:
        st.markdown("### Manage Your Watchlist")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### ➕ Add New Profile")

            with st.form("add_profile_form"):
                platform_choice = st.selectbox(
                    "Platform",
                    ["TikTok", "Twitter", "Reddit"],
                    help="Select the social media platform"
                )

                platform_map = {'TikTok': 'tiktok', 'Twitter': 'twitter', 'Reddit': 'reddit'}
                platform_key = platform_map[platform_choice]

                placeholder_map = {
                    'tiktok': 'e.g., charlidamelio (without @)',
                    'twitter': 'e.g., elonmusk (without @)',
                    'reddit': 'e.g., science (subreddit or user)'
                }

                new_username = st.text_input(
                    f"{platform_choice} Username",
                    placeholder=placeholder_map[platform_key],
                    help=f"Enter the {platform_choice} username without the @ symbol"
                )

                submit_add = st.form_submit_button("Add to Watchlist", use_container_width=True)

                if submit_add and new_username:
                    with st.spinner(f"Adding @{new_username} ({platform_choice})..."):
                        success, message = add_profile_to_watchlist(new_username, platform_key)
                        if success:
                            st.success(message)
                            st.toast(f"✅ @{new_username} added! Refresh the page to see updates.", icon="🎉")
                        else:
                            st.error(message)
        
        with col2:
            st.markdown("#### ➖ Remove Profile")
            
            if not profiles_df.empty:
                with st.form("remove_profile_form"):
                    remove_username = st.selectbox(
                        "Select profile to remove",
                        options=profiles_df['username'].tolist(),
                        format_func=lambda x: f"@{x}"
                    )
                    
                    submit_remove = st.form_submit_button("Remove from Watchlist", use_container_width=True)
                    
                    if submit_remove and remove_username:
                        success, message = remove_profile_from_watchlist(remove_username)
                        if success:
                            st.success(message)
                            st.toast(f"🗑️ Profile removed! Refresh the page to see updates.", icon="✅")
                        else:
                            st.error(message)
            else:
                st.info("No profiles to remove")
        
        st.markdown("---")
        st.markdown("#### 📋 Current Watchlist")
        
        if not profiles_df.empty:
            # Display current profiles in a nice table (platform-aware)
            watchlist_df = profiles_df[['platform', 'username', 'display_name', 'followers', 'videos', 'avg_views', 'last_updated']].copy()
            watchlist_df.columns = ['Platform', 'Username', 'Display Name', 'Followers', 'Videos', 'Avg Views', 'Last Updated']

            # Platform icons
            platform_icons = {'tiktok': '🎵', 'twitter': '🐦', 'reddit': '🔴'}
            watchlist_df['Platform'] = watchlist_df['Platform'].apply(lambda x: f"{platform_icons.get(x, '📱')} {x.title()}")

            watchlist_df['Username'] = watchlist_df['Username'].apply(lambda x: f"@{x}")
            watchlist_df['Followers'] = watchlist_df['Followers'].apply(lambda x: f"{x:,}")
            watchlist_df['Avg Views'] = watchlist_df['Avg Views'].apply(lambda x: f"{int(x):,}" if x else "0")
            watchlist_df['Last Updated'] = pd.to_datetime(watchlist_df['Last Updated']).dt.strftime('%Y-%m-%d %H:%M')
            
            st.dataframe(watchlist_df, use_container_width=True, hide_index=True)
        else:
            st.info("Your watchlist is empty. Add TikTok profiles above to start tracking.")
        
        # System Status
        st.markdown("---")
        st.markdown("#### 🔧 System Status")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            db_status = "🟢 Connected" if engine else "🔴 Disconnected"
            st.markdown(f"**Database:** {db_status}")
        
        with col2:
            api_key = os.getenv("RAPIDAPI_KEY", "")
            api_status = "🟢 Configured" if api_key else "🔴 Not Set"
            st.markdown(f"**RapidAPI:** {api_status}")
        
        with col3:
            telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
            telegram_status = "🟢 Configured" if telegram_token else "🔴 Not Set"
            st.markdown(f"**Telegram:** {telegram_status}")
        
        with col4:
            st.markdown(f"**Schema:** v{schema_health.get('schema_version', 'unknown')}")


# =============================================================================
# SIDEBAR
# =============================================================================

def render_sidebar():
    """Render the sidebar with additional options."""
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 1rem 0;">
            <span style="font-size: 2rem;">📊</span>
            <h2 style="margin: 0.5rem 0; font-size: 1.5rem;">Pulse</h2>
            <p style="color: #a0a0b0; font-size: 0.85rem;">Analytics Dashboard</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Quick stats
        stats = get_aggregate_stats()
        
        st.markdown("### Quick Stats")
        st.metric("Profiles", stats.get('total_profiles', 0))
        st.metric("Total Followers", format_number(stats.get('total_followers', 0)))
        st.metric("Viral Posts", stats.get('viral_posts', 0))
        
        st.markdown("---")
        
        # Refresh button
        if st.button("🔄 Refresh Data", use_container_width=True):
            get_all_profiles.clear()
            get_all_posts.clear()
            get_aggregate_stats.clear()
            st.rerun()
        
        st.markdown("---")
        
        # Info
        st.markdown("""
        <div style="font-size: 0.8rem; color: #a0a0b0;">
            <p><strong>Update Schedule:</strong><br>Every 6 hours</p>
            <p><strong>Viral Threshold:</strong><br>5x average views</p>
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    logger.info("Starting Pulse Dashboard")
    render_sidebar()
    main()
