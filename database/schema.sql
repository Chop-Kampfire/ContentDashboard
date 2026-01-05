-- ============================================
-- Pulse - Multi-Platform Analytics Dashboard
-- PostgreSQL Database Schema
-- Version 0.0.2 - Supports TikTok, Twitter/X, Reddit
-- ============================================

-- ===========================================
-- ENUM TYPES
-- ===========================================

-- Platform type enum
DO $$ BEGIN
    CREATE TYPE platform_type AS ENUM ('tiktok', 'twitter', 'reddit');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- User role type enum
DO $$ BEGIN
    CREATE TYPE user_role_type AS ENUM ('creator', 'moderator', 'power_user', 'brand');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


-- ===========================================
-- PROFILES TABLE
-- ===========================================

CREATE TABLE IF NOT EXISTS profiles (
    id SERIAL PRIMARY KEY,
    
    -- Platform identification
    platform platform_type NOT NULL DEFAULT 'tiktok',
    platform_user_id VARCHAR(64),  -- Platform's internal ID
    username VARCHAR(64) NOT NULL,
    
    -- User categorization
    user_role user_role_type DEFAULT 'creator',
    
    -- Profile info (common across platforms)
    display_name VARCHAR(128),
    bio TEXT,
    avatar_url TEXT,
    
    -- Common metrics (latest snapshot)
    follower_count BIGINT DEFAULT 0,
    following_count BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    video_count INTEGER DEFAULT 0,
    
    -- Reddit-specific fields
    subreddit_name VARCHAR(128),        -- For subreddit profiles (r/name)
    subreddit_subscribers BIGINT,       -- Subreddit subscriber count
    active_users INTEGER,               -- Currently active users
    
    -- Calculated metrics for viral detection
    average_post_views FLOAT DEFAULT 0.0,
    
    -- Tracking flags
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped_at TIMESTAMP,
    
    -- Constraints
    CONSTRAINT uq_profile_username_platform UNIQUE (username, platform),
    CONSTRAINT uq_profile_platform_id UNIQUE (platform_user_id, platform)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_profiles_username ON profiles(username);
CREATE INDEX IF NOT EXISTS idx_profiles_platform ON profiles(platform);
CREATE INDEX IF NOT EXISTS idx_profiles_active ON profiles(is_active);
CREATE INDEX IF NOT EXISTS idx_profiles_active_platform ON profiles(is_active, platform);


-- ===========================================
-- PROFILE HISTORY TABLE
-- ===========================================

CREATE TABLE IF NOT EXISTS profile_history (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER REFERENCES profiles(id) ON DELETE CASCADE,
    
    -- Common metrics snapshot
    follower_count BIGINT DEFAULT 0,
    following_count BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    video_count INTEGER DEFAULT 0,
    
    -- Reddit-specific history
    subreddit_subscribers BIGINT,
    active_users INTEGER,
    
    -- Delta since last record
    follower_change INTEGER DEFAULT 0,
    likes_change INTEGER DEFAULT 0,
    
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_profile_history_lookup 
    ON profile_history(profile_id, recorded_at);


-- ===========================================
-- POSTS TABLE
-- ===========================================

CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER REFERENCES profiles(id) ON DELETE CASCADE,
    
    -- Platform identification
    platform platform_type NOT NULL DEFAULT 'tiktok',
    platform_post_id VARCHAR(64) NOT NULL,
    
    -- Content info (common across platforms)
    description TEXT,
    video_url TEXT,
    thumbnail_url TEXT,
    duration_seconds INTEGER,
    
    -- Common engagement metrics
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    share_count BIGINT DEFAULT 0,
    
    -- Twitter-specific fields
    retweet_count BIGINT,           -- Number of retweets
    quote_count BIGINT,             -- Number of quote tweets
    bookmark_count BIGINT,          -- Number of bookmarks
    impression_count BIGINT,        -- Number of impressions
    
    -- Reddit-specific fields
    upvote_ratio FLOAT,             -- Upvote percentage (0.0-1.0)
    is_crosspost BOOLEAN,           -- Whether post is a crosspost
    original_subreddit VARCHAR(128), -- Source subreddit if crosspost
    reddit_score INTEGER,           -- Reddit score (upvotes - downvotes)
    
    -- Viral detection
    is_viral BOOLEAN DEFAULT FALSE,
    viral_alert_sent BOOLEAN DEFAULT FALSE,
    
    -- Timestamps
    posted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT uq_post_platform_id UNIQUE (platform_post_id, platform)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id ON posts(platform_post_id);
CREATE INDEX IF NOT EXISTS idx_posts_profile_posted ON posts(profile_id, posted_at);
CREATE INDEX IF NOT EXISTS idx_posts_viral ON posts(is_viral, viral_alert_sent);
CREATE INDEX IF NOT EXISTS idx_posts_platform ON posts(platform);


-- ===========================================
-- POST HISTORY TABLE
-- ===========================================

CREATE TABLE IF NOT EXISTS post_history (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
    
    -- Common metrics snapshot
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    share_count BIGINT DEFAULT 0,
    
    -- Twitter-specific metrics
    retweet_count BIGINT,
    quote_count BIGINT,
    
    -- Reddit-specific metrics
    upvote_ratio FLOAT,
    reddit_score INTEGER,
    
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_post_history_lookup 
    ON post_history(post_id, recorded_at);


-- ===========================================
-- ALERT LOGS TABLE
-- ===========================================

CREATE TABLE IF NOT EXISTS alert_logs (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES posts(id) ON DELETE SET NULL,
    profile_id INTEGER REFERENCES profiles(id) ON DELETE SET NULL,
    
    -- Platform context
    platform platform_type,
    
    alert_type VARCHAR(32) NOT NULL,
    message TEXT NOT NULL,
    
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_alert_logs_type ON alert_logs(alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_logs_sent ON alert_logs(sent_at);
CREATE INDEX IF NOT EXISTS idx_alert_logs_platform ON alert_logs(platform);


-- ============================================
-- VIEWS FOR DASHBOARD QUERIES
-- ============================================

-- Latest profile metrics with 24h change (multi-platform)
CREATE OR REPLACE VIEW v_profile_metrics AS
SELECT 
    p.id,
    p.platform,
    p.username,
    p.display_name,
    p.avatar_url,
    p.user_role,
    p.follower_count,
    p.total_likes,
    p.video_count,
    p.average_post_views,
    p.subreddit_name,
    p.subreddit_subscribers,
    p.active_users,
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


-- Top performing posts (last 30 days, multi-platform)
CREATE OR REPLACE VIEW v_top_posts AS
SELECT 
    po.id,
    po.platform,
    po.platform_post_id,
    po.description,
    po.view_count,
    po.like_count,
    po.share_count,
    po.comment_count,
    po.retweet_count,
    po.quote_count,
    po.upvote_ratio,
    po.reddit_score,
    po.is_viral,
    po.posted_at,
    pr.username,
    pr.display_name,
    pr.avatar_url,
    pr.user_role,
    CASE 
        WHEN pr.average_post_views > 0 
        THEN ROUND((po.view_count::FLOAT / pr.average_post_views)::NUMERIC, 2)
        ELSE 0 
    END as performance_ratio
FROM posts po
JOIN profiles pr ON po.profile_id = pr.id
WHERE po.posted_at >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY po.view_count DESC;


-- Platform-specific post summary
CREATE OR REPLACE VIEW v_platform_stats AS
SELECT 
    platform,
    COUNT(DISTINCT profile_id) as profile_count,
    COUNT(*) as post_count,
    SUM(view_count) as total_views,
    SUM(like_count) as total_likes,
    AVG(view_count)::BIGINT as avg_views_per_post,
    COUNT(*) FILTER (WHERE is_viral = TRUE) as viral_post_count
FROM posts
WHERE posted_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY platform;
