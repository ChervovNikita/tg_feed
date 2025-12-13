#!/usr/bin/env python3
"""Script to seed demo data for testing."""
import os
import sys
import random
from datetime import datetime, timedelta

import psycopg2
import numpy as np

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://postgres:postgres@localhost:5432/tg_filter'
)

EMBEDDING_DIM = 1536


def generate_random_embedding():
    """Generate random normalized embedding."""
    emb = np.random.randn(EMBEDDING_DIM).astype(np.float32)
    emb = emb / np.linalg.norm(emb)
    return emb.tolist()


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Create demo users
    print("Creating demo users...")
    demo_users = [
        (100001, 'demo_user_1'),
        (100002, 'demo_user_2'),
        (100003, 'demo_user_3'),
    ]
    
    for user_id, username in demo_users:
        cursor.execute(
            "INSERT INTO users (id, username) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
            (user_id, username)
        )
    
    # Create demo channels
    print("Creating demo subscriptions...")
    channels = [
        (-1001234567890, 'tech_news', 'technews'),
        (-1001234567891, 'python_tips', 'pythontips'),
        (-1001234567892, 'ml_daily', 'mldaily'),
    ]
    
    for user_id, _ in demo_users:
        for channel_id, name, username in channels:
            cursor.execute(
                """
                INSERT INTO subscriptions (user_id, channel_id, channel_name, channel_username)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, channel_id) DO NOTHING
                """,
                (user_id, channel_id, name, username)
            )
    
    # Create demo posts
    print("Creating demo posts...")
    posts = []
    for i in range(100):
        channel_id = channels[i % 3][0]
        message_id = 1000 + i
        text = f"Demo post #{i}: This is a sample post about technology and programming."
        
        cursor.execute(
            """
            INSERT INTO posts (channel_id, message_id, text, text_embedding, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (channel_id, message_id) DO UPDATE SET id = posts.id
            RETURNING id
            """,
            (
                channel_id,
                message_id,
                text,
                generate_random_embedding(),
                datetime.now() - timedelta(hours=random.randint(0, 168))
            )
        )
        post_id = cursor.fetchone()[0]
        posts.append(post_id)
    
    # Create demo predictions
    print("Creating demo predictions...")
    for post_id in posts:
        for user_id, _ in demo_users:
            score = random.random()
            sent = score > 0.5
            
            cursor.execute(
                """
                INSERT INTO predictions (user_id, post_id, score, sent, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    user_id,
                    post_id,
                    score,
                    sent,
                    datetime.now() - timedelta(hours=random.randint(0, 168))
                )
            )
    
    # Create demo reactions
    print("Creating demo reactions...")
    for post_id in random.sample(posts, min(50, len(posts))):
        for user_id, _ in demo_users:
            if random.random() > 0.5:
                reaction = random.choice([1, -1])
                cursor.execute(
                    """
                    INSERT INTO reactions (user_id, post_id, reaction, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, post_id) DO NOTHING
                    """,
                    (
                        user_id,
                        post_id,
                        reaction,
                        datetime.now() - timedelta(hours=random.randint(0, 168))
                    )
                )
    
    # Create demo metrics
    print("Creating demo metrics...")
    metric_names = [
        'hourly_predictions',
        'hourly_sent_predictions',
        'daily_precision',
        'engagement_rate',
        'daily_active_users',
    ]
    
    for i in range(168):  # 7 days of hourly metrics
        for metric_name in metric_names:
            cursor.execute(
                """
                INSERT INTO metrics (metric_name, metric_value, created_at)
                VALUES (%s, %s, %s)
                """,
                (
                    metric_name,
                    random.random() * 100,
                    datetime.now() - timedelta(hours=i)
                )
            )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print()
    print("=" * 50)
    print("Demo data created successfully!")
    print("=" * 50)
    print()
    print("Created:")
    print(f"  - {len(demo_users)} users")
    print(f"  - {len(channels)} channels per user")
    print(f"  - {len(posts)} posts")
    print(f"  - Multiple predictions and reactions")
    print(f"  - 7 days of metric history")


if __name__ == "__main__":
    main()

