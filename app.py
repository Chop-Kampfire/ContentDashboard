"""
Pulse - TikTok Analytics Dashboard
Main Streamlit Application

A sleek, investment-style analytics dashboard for tracking TikTok performance.
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, func, desc
from sqlalchemy.orm import sessionmaker

from database.models import Base, Profile, ProfileHistory, Post, PostHistory, AlertLog

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# PAGE CONFIG & STYLING
# =============================================================================

st.set_page_config(
    page_title="Pulse • TikTok Analytics",
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
        st.error("⚠️ DATABASE_URL environment variable not set")
        return None
    
    # Fix postgres:// to postgresql:// for SQLAlchemy 2.0+
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    try:
        engine = create_engine(database_url, pool_pre_ping=True)
        Base.metadata.create_all(bind=engine)
        return engine
    except Exception as e:
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
        
        # #region agent log
        print(f"[PULSE DEBUG] get_all_profiles: Found {len(profiles)} active profiles: {[p.username for p in profiles]}", flush=True)
        # #endregion
        
        data = [{
            "id": p.id,
            "username": p.username,
            "display_name": p.display_name or p.username,
            "avatar_url": p.avatar_url,
            "followers": p.follower_count,
            "total_likes": p.total_likes,
            "videos": p.video_count,
            "avg_views": p.average_post_views,
            "last_updated": p.last_scraped_at
        } for p in profiles]
        
        return pd.DataFrame(data)
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
        
        data = [{
            "id": p.Post.id,
            "post_id": p.Post.tiktok_post_id,
            "username": f"@{p.Profile.username}",
            "description": (p.Post.description or "")[:80] + "..." if p.Post.description and len(p.Post.description) > 80 else (p.Post.description or ""),
            "views": p.Post.view_count,
            "likes": p.Post.like_count,
            "comments": p.Post.comment_count,
            "shares": p.Post.share_count,
            "is_viral": p.Post.is_viral,
            "posted_at": p.Post.posted_at,
            "avg_views": p.Profile.average_post_views,
            "efficacy_score": round((p.Post.view_count / p.Profile.average_post_views * 100), 1) if p.Profile.average_post_views > 0 else 0
        } for p in posts]
        
        return pd.DataFrame(data)
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


def _debug_log(data: dict):
    """Helper to write debug logs - outputs to stdout for Railway logs."""
    import json, time, sys
    log_msg = json.dumps({**data, "timestamp": time.time()*1000})
    
    # Print to stdout with flush (visible in Railway logs)
    print(f"[PULSE DEBUG] {log_msg}", flush=True)
    sys.stdout.flush()
    
    # Also store in session state for UI display
    if 'debug_logs' not in st.session_state:
        st.session_state.debug_logs = []
    st.session_state.debug_logs.append(data.get('message', log_msg))

def add_profile_to_watchlist(username: str) -> tuple[bool, str]:
    """Add a new profile to the watchlist."""
    from scraper import TikTokScraper
    
    # #region agent log
    _debug_log({"hypothesisId":"F","location":"app.py:add_profile_to_watchlist:start","message":"Starting add profile","data":{"username":username}})
    # #endregion
    
    try:
        # #region agent log
        print(f"[PULSE DEBUG] Creating TikTokScraper instance...", flush=True)
        # #endregion
        scraper = TikTokScraper()
        
        # #region agent log
        print(f"[PULSE DEBUG] Creating event loop...", flush=True)
        # #endregion
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # #region agent log
        print(f"[PULSE DEBUG] Calling scraper.add_profile() for '{username}'...", flush=True)
        # #endregion
        
        try:
            profile = loop.run_until_complete(scraper.add_profile(username, send_notification=True))
            print(f"[PULSE DEBUG] scraper.add_profile() returned successfully!", flush=True)
        except Exception as scraper_err:
            print(f"[PULSE DEBUG] scraper.add_profile() FAILED: {type(scraper_err).__name__}: {scraper_err}", flush=True)
            raise
        finally:
            loop.close()
        
        # #region agent log
        _debug_log({"hypothesisId":"F","location":"app.py:add_profile_to_watchlist:after_scraper","message":"Profile returned from scraper","data":{"profile_id":profile.id,"profile_username":profile.username,"is_active":profile.is_active}})
        # #endregion
        
        # Clear cache to refresh data
        get_all_profiles.clear()
        get_aggregate_stats.clear()
        get_all_posts.clear()  # Also clear posts cache
        
        # #region agent log
        _debug_log({"hypothesisId":"F","location":"app.py:add_profile_to_watchlist:cache_cleared","message":"All caches cleared"})
        # #endregion
        
        # Verify data can be fetched immediately
        # #region agent log
        fresh_profiles = get_all_profiles()
        profile_count = len(fresh_profiles)
        usernames_list = fresh_profiles['username'].tolist() if not fresh_profiles.empty else []
        _debug_log({"hypothesisId":"F","location":"app.py:add_profile_to_watchlist:verify_fetch","message":"Fetched profiles after cache clear","data":{"profile_count":profile_count,"usernames":usernames_list}})
        print(f"[PULSE DEBUG] Verification - Fresh profiles count: {profile_count}, usernames: {usernames_list}", flush=True)
        # #endregion
        
        return True, f"Successfully added @{profile.username} (DB has {profile_count} profiles)"
    except Exception as e:
        # #region agent log
        import traceback
        tb = traceback.format_exc()
        _debug_log({"hypothesisId":"F","location":"app.py:add_profile_to_watchlist:error","message":"Exception occurred","data":{"error":str(e),"error_type":type(e).__name__}})
        print(f"[PULSE DEBUG ERROR] {type(e).__name__}: {e}\n{tb}", flush=True)
        # #endregion
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
        <div class="pulse-subtitle">TikTok Analytics Dashboard • Real-time performance tracking</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Check database connection
    engine = get_database_engine()
    if not engine:
        st.warning("⚠️ Please configure DATABASE_URL environment variable to connect to PostgreSQL.")
        st.code("DATABASE_URL=postgresql://user:password@host:port/database")
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
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                search_term = st.text_input("🔍 Search posts", placeholder="Search by username or description...")
            
            with col2:
                viral_filter = st.selectbox("Filter", ["All Posts", "Viral Only 🔥", "Below Average"])
            
            with col3:
                sort_by = st.selectbox("Sort by", ["Views (High to Low)", "Efficacy Score", "Most Recent"])
            
            # Apply filters
            filtered_df = posts_df.copy()
            
            if search_term:
                filtered_df = filtered_df[
                    filtered_df['username'].str.contains(search_term, case=False) |
                    filtered_df['description'].str.contains(search_term, case=False, na=False)
                ]
            
            if viral_filter == "Viral Only 🔥":
                filtered_df = filtered_df[filtered_df['is_viral'] == True]
            elif viral_filter == "Below Average":
                filtered_df = filtered_df[filtered_df['efficacy_score'] < 100]
            
            # Sort
            if sort_by == "Views (High to Low)":
                filtered_df = filtered_df.sort_values('views', ascending=False)
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
            
            # Display table
            display_cols = ['username', 'description', 'views', 'likes', 'comments', 'shares', 'efficacy_score', 'is_viral', 'posted_at']
            
            # Format for display
            display_df = filtered_df[display_cols].copy()
            display_df.columns = ['Account', 'Description', 'Views', 'Likes', 'Comments', 'Shares', 'Efficacy %', 'Viral 🔥', 'Posted']
            display_df['Views'] = display_df['Views'].apply(lambda x: f"{x:,}")
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
                        engagement_data = {
                            'Metric': ['Views', 'Likes', 'Comments', 'Shares'],
                            'Count': [post_data['views'], post_data['likes'], post_data['comments'], post_data['shares']]
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
                new_username = st.text_input(
                    "TikTok Username",
                    placeholder="e.g., charlidamelio (without @)",
                    help="Enter the TikTok username without the @ symbol"
                )
                
                submit_add = st.form_submit_button("Add to Watchlist", use_container_width=True)
                
                if submit_add and new_username:
                    with st.spinner(f"Adding @{new_username}..."):
                        success, message = add_profile_to_watchlist(new_username)
                        if success:
                            st.success(message)
                            st.rerun()
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
                            st.rerun()
                        else:
                            st.error(message)
            else:
                st.info("No profiles to remove")
        
        st.markdown("---")
        st.markdown("#### 📋 Current Watchlist")
        
        if not profiles_df.empty:
            # Display current profiles in a nice table
            watchlist_df = profiles_df[['username', 'display_name', 'followers', 'videos', 'avg_views', 'last_updated']].copy()
            watchlist_df.columns = ['Username', 'Display Name', 'Followers', 'Videos', 'Avg Views', 'Last Updated']
            watchlist_df['Username'] = watchlist_df['Username'].apply(lambda x: f"@{x}")
            watchlist_df['Followers'] = watchlist_df['Followers'].apply(lambda x: f"{x:,}")
            watchlist_df['Avg Views'] = watchlist_df['Avg Views'].apply(lambda x: f"{int(x):,}" if x else "0")
            watchlist_df['Last Updated'] = pd.to_datetime(watchlist_df['Last Updated']).dt.strftime('%Y-%m-%d %H:%M')
            
            st.dataframe(watchlist_df, use_container_width=True, hide_index=True)
        else:
            st.info("Your watchlist is empty. Add TikTok profiles above to start tracking.")
        
        # API Status
        st.markdown("---")
        st.markdown("#### 🔧 System Status")
        
        col1, col2, col3 = st.columns(3)
        
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
            <p style="color: #a0a0b0; font-size: 0.85rem;">TikTok Analytics</p>
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
    render_sidebar()
    main()

