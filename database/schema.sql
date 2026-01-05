-- ============================================
-- Pulse - TikTok Analytics Dashboard
-- PostgreSQL Database Schema
-- ============================================

-- Profiles: TikTok accounts on the watchlist
CREATE TABLE IF NOT EXISTS profiles (
    id SERIAL PRIMARY KEY,
    
    -- TikTok identifiers
    tiktok_user_id VARCHAR(64) UNIQUE,
    username VARCHAR(64) UNIQUE NOT NULL,
    
    -- Profile info
    display_name VARCHAR(128),
    bio TEXT,
    avatar_url TEXT,
    
    -- Current metrics (latest snapshot)
    follower_count BIGINT DEFAULT 0,
    following_count BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    video_count INTEGER DEFAULT 0,
    
    -- Calculated metrics for viral detection
    average_post_views FLOAT DEFAULT 0.0,
    
    -- Tracking flags
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_profiles_username ON profiles(username);
CREATE INDEX IF NOT EXISTS idx_profiles_active ON profiles(is_active);


-- Profile History: Time-series for follower growth charts
CREATE TABLE IF NOT EXISTS profile_history (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER REFERENCES profiles(id) ON DELETE CASCADE,
    
    -- Metrics snapshot
    follower_count BIGINT DEFAULT 0,
    following_count BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    video_count INTEGER DEFAULT 0,
    
    -- Delta since last record
    follower_change INTEGER DEFAULT 0,
    likes_change INTEGER DEFAULT 0,
    
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_profile_history_lookup 
    ON profile_history(profile_id, recorded_at);


-- Posts: Individual TikTok videos
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER REFERENCES profiles(id) ON DELETE CASCADE,
    
    -- TikTok identifiers
    tiktok_post_id VARCHAR(64) UNIQUE NOT NULL,
    
    -- Content info
    description TEXT,
    video_url TEXT,
    thumbnail_url TEXT,
    duration_seconds INTEGER,
    
    -- Engagement metrics
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    share_count BIGINT DEFAULT 0,
    
    -- Viral detection
    is_viral BOOLEAN DEFAULT FALSE,
    viral_alert_sent BOOLEAN DEFAULT FALSE,
    
    -- Timestamps
    posted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_posts_tiktok_id ON posts(tiktok_post_id);
CREATE INDEX IF NOT EXISTS idx_posts_profile_posted ON posts(profile_id, posted_at);
CREATE INDEX IF NOT EXISTS idx_posts_viral ON posts(is_viral, viral_alert_sent);


-- Post History: Time-series for post view growth
CREATE TABLE IF NOT EXISTS post_history (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
    
    -- Metrics snapshot
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    share_count BIGINT DEFAULT 0,
    
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_post_history_lookup 
    ON post_history(post_id, recorded_at);


-- Alert Logs: Track sent Telegram notifications
CREATE TABLE IF NOT EXISTS alert_logs (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES posts(id) ON DELETE SET NULL,
    profile_id INTEGER REFERENCES profiles(id) ON DELETE SET NULL,
    
    alert_type VARCHAR(32) NOT NULL,
    message TEXT NOT NULL,
    
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_alert_logs_type ON alert_logs(alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_logs_sent ON alert_logs(sent_at);


-- ============================================
-- Useful Views for Dashboard Queries
-- ============================================

-- Latest profile metrics with 24h change
CREATE OR REPLACE VIEW v_profile_metrics AS
SELECT 
    p.id,
    p.username,
    p.display_name,
    p.avatar_url,
    p.follower_count,
    p.total_likes,
    p.video_count,
    p.average_post_views,
    p.last_scraped_at,
    COALESCE(
        p.follower_count - LAG(h.follower_count) OVER (
            PARTITION BY p.id ORDER BY h.recorded_at
        ), 
        0
    ) as follower_change_24h
FROM profiles p
LEFT JOIN profile_history h ON p.id = h.profile_id
WHERE p.is_active = TRUE;


-- Top performing posts (last 30 days)
CREATE OR REPLACE VIEW v_top_posts AS
SELECT 
    po.id,
    po.tiktok_post_id,
    po.description,
    po.view_count,
    po.like_count,
    po.share_count,
    po.is_viral,
    po.posted_at,
    pr.username,
    pr.display_name,
    pr.avatar_url,
    CASE 
        WHEN pr.average_post_views > 0 
        THEN ROUND((po.view_count::FLOAT / pr.average_post_views)::NUMERIC, 2)
        ELSE 0 
    END as performance_ratio
FROM posts po
JOIN profiles pr ON po.profile_id = pr.id
WHERE po.posted_at >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY po.view_count DESC;

