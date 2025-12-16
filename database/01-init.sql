-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Users table
CREATE TABLE users (
    id BIGINT PRIMARY KEY,  -- telegram user_id
    username TEXT,
    waiting_for_posts BOOLEAN DEFAULT FALSE,  -- true if user is waiting for new posts
    created_at TIMESTAMP DEFAULT NOW()
);

-- User tag subscriptions (which Medium tags to follow)
CREATE TABLE tag_subscriptions (
    user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, tag)
);

-- Posts/Articles from Medium
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    source TEXT DEFAULT 'medium',  -- 'medium', 'telegram', etc.
    source_id TEXT,  -- article_id for Medium
    source_url TEXT UNIQUE,  -- URL to original article
    title TEXT,
    text TEXT,
    author TEXT,
    tag TEXT,  -- Medium tag
    media_urls TEXT[],
    text_embedding vector(1536),
    image_embedding vector(1536),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for vector similarity search
CREATE INDEX ON posts USING ivfflat (text_embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX idx_posts_source_url ON posts(source_url);
CREATE INDEX idx_posts_tag ON posts(tag);

-- Predictions made by the model
CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    post_id INT REFERENCES posts(id) ON DELETE CASCADE,
    score FLOAT NOT NULL,
    sent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_predictions_user_id ON predictions(user_id);
CREATE INDEX idx_predictions_created_at ON predictions(created_at);

-- User reactions (feedback)
CREATE TABLE reactions (
    user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    post_id INT REFERENCES posts(id) ON DELETE CASCADE,
    reaction SMALLINT NOT NULL,  -- 1=like, -1=dislike
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, post_id)
);

-- Indexes for faster reaction queries
CREATE INDEX idx_reactions_user_id_created_at ON reactions(user_id, created_at DESC);
CREATE INDEX idx_reactions_user_id_reaction ON reactions(user_id, reaction) WHERE reaction > 0;  -- For positive reactions
CREATE INDEX idx_reactions_user_id_reaction_neg ON reactions(user_id, reaction) WHERE reaction < 0;  -- For negative reactions

-- User ML models (serialized weights)
CREATE TABLE user_models (
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    model_weights BYTEA,  -- pickled LogisticRegression
    threshold FLOAT DEFAULT 0.5,
    accuracy FLOAT,
    num_samples INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Metrics table for monitoring
CREATE TABLE metrics (
    id SERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_value FLOAT NOT NULL,
    labels JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_metrics_name ON metrics(metric_name);
CREATE INDEX idx_metrics_created_at ON metrics(created_at);

-- ============================================
-- ВИТРИНЫ (Data Marts) - заполняются Airflow
-- ============================================

-- Витрина: ежедневная статистика по пользователям
CREATE TABLE dm_user_daily_stats (
    user_id BIGINT NOT NULL,
    date DATE NOT NULL,
    posts_received INT DEFAULT 0,
    posts_sent INT DEFAULT 0,
    likes INT DEFAULT 0,
    dislikes INT DEFAULT 0,
    engagement_rate FLOAT DEFAULT 0,
    avg_score FLOAT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, date)
);

-- Витрина: статистика по тегам
CREATE TABLE dm_tag_stats (
    tag TEXT NOT NULL,
    date DATE NOT NULL,
    total_posts INT DEFAULT 0,
    total_predictions INT DEFAULT 0,
    posts_sent INT DEFAULT 0,
    avg_score FLOAT DEFAULT 0,
    positive_reactions INT DEFAULT 0,
    negative_reactions INT DEFAULT 0,
    subscribers INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tag, date)
);

-- Витрина: эффективность моделей
CREATE TABLE dm_model_performance (
    user_id BIGINT NOT NULL,
    date DATE NOT NULL,
    predictions_count INT DEFAULT 0,
    true_positives INT DEFAULT 0,
    false_positives INT DEFAULT 0,
    true_negatives INT DEFAULT 0,
    false_negatives INT DEFAULT 0,
    precision FLOAT DEFAULT 0,
    recall FLOAT DEFAULT 0,
    f1_score FLOAT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, date)
);

CREATE INDEX idx_dm_user_daily_date ON dm_user_daily_stats(date);
CREATE INDEX idx_dm_tag_stats_date ON dm_tag_stats(date);
CREATE INDEX idx_dm_model_perf_date ON dm_model_performance(date);

-- Insert default demo user for testing
INSERT INTO users (id, username) VALUES (1, 'demo_user');
