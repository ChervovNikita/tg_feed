"""DAG for building data marts (витрины)."""
import os
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

from airflow import DAG
from airflow.operators.python import PythonOperator

# Configuration
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/tg_filter')


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(DATABASE_URL)


def build_user_daily_stats(**context):
    """
    Витрина: ежедневная статистика по пользователям.
    
    Агрегирует данные за вчера:
    - Сколько статей получил пользователь
    - Сколько было отправлено (прошло фильтр)
    - Сколько лайков/дизлайков поставил
    - Engagement rate
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Берём данные за вчера (или указанную дату)
    target_date = context.get('ds')  # execution date from Airflow
    
    cursor.execute("""
        INSERT INTO dm_user_daily_stats (
            user_id, date, posts_received, posts_sent, 
            likes, dislikes, engagement_rate, avg_score, updated_at
        )
        SELECT 
            p.user_id,
            DATE(p.created_at) as date,
            COUNT(*) as posts_received,
            COUNT(*) FILTER (WHERE p.sent = TRUE) as posts_sent,
            COALESCE(r.likes, 0) as likes,
            COALESCE(r.dislikes, 0) as dislikes,
            CASE 
                WHEN COUNT(*) FILTER (WHERE p.sent = TRUE) > 0 
                THEN COALESCE(r.likes + r.dislikes, 0)::FLOAT / COUNT(*) FILTER (WHERE p.sent = TRUE)
                ELSE 0 
            END as engagement_rate,
            AVG(p.score) as avg_score,
            NOW()
        FROM predictions p
        LEFT JOIN (
            SELECT 
                user_id,
                DATE(created_at) as date,
                COUNT(*) FILTER (WHERE reaction > 0) as likes,
                COUNT(*) FILTER (WHERE reaction < 0) as dislikes
            FROM reactions
            WHERE DATE(created_at) = %s
            GROUP BY user_id, DATE(created_at)
        ) r ON p.user_id = r.user_id AND DATE(p.created_at) = r.date
        WHERE DATE(p.created_at) = %s
        GROUP BY p.user_id, DATE(p.created_at), r.likes, r.dislikes
        ON CONFLICT (user_id, date) 
        DO UPDATE SET
            posts_received = EXCLUDED.posts_received,
            posts_sent = EXCLUDED.posts_sent,
            likes = EXCLUDED.likes,
            dislikes = EXCLUDED.dislikes,
            engagement_rate = EXCLUDED.engagement_rate,
            avg_score = EXCLUDED.avg_score,
            updated_at = NOW()
    """, (target_date, target_date))
    
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Built dm_user_daily_stats for {target_date}: {affected} rows")


def build_tag_stats(**context):
    """
    Витрина: статистика по тегам.
    
    Агрегирует:
    - Количество статей по тегу
    - Средний скор релевантности
    - Реакции на статьи тега
    - Количество подписчиков на тег
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    target_date = context.get('ds')
    
    cursor.execute("""
        INSERT INTO dm_tag_stats (
            tag, date, total_posts, total_predictions, posts_sent,
            avg_score, positive_reactions, negative_reactions, subscribers, updated_at
        )
        SELECT 
            po.tag,
            DATE(po.created_at) as date,
            COUNT(DISTINCT po.id) as total_posts,
            COUNT(pr.id) as total_predictions,
            COUNT(pr.id) FILTER (WHERE pr.sent = TRUE) as posts_sent,
            AVG(pr.score) as avg_score,
            COUNT(r.post_id) FILTER (WHERE r.reaction > 0) as positive_reactions,
            COUNT(r.post_id) FILTER (WHERE r.reaction < 0) as negative_reactions,
            (
                SELECT COUNT(DISTINCT user_id) 
                FROM tag_subscriptions 
                WHERE tag = po.tag AND is_active = TRUE
            ) as subscribers,
            NOW()
        FROM posts po
        LEFT JOIN predictions pr ON po.id = pr.post_id
        LEFT JOIN reactions r ON po.id = r.post_id
        WHERE DATE(po.created_at) = %s AND po.tag IS NOT NULL
        GROUP BY po.tag, DATE(po.created_at)
        ON CONFLICT (tag, date)
        DO UPDATE SET
            total_posts = EXCLUDED.total_posts,
            total_predictions = EXCLUDED.total_predictions,
            posts_sent = EXCLUDED.posts_sent,
            avg_score = EXCLUDED.avg_score,
            positive_reactions = EXCLUDED.positive_reactions,
            negative_reactions = EXCLUDED.negative_reactions,
            subscribers = EXCLUDED.subscribers,
            updated_at = NOW()
    """, (target_date,))
    
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Built dm_tag_stats for {target_date}: {affected} rows")


def build_model_performance(**context):
    """
    Витрина: эффективность ML моделей.
    
    Считает precision/recall/F1 для каждого пользователя:
    - True Positive: sent=TRUE, reaction=1 (лайк)
    - False Positive: sent=TRUE, reaction=-1 (дизлайк)
    - False Negative: sent=FALSE, но пользователь бы лайкнул (оцениваем косвенно)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    target_date = context.get('ds')
    
    cursor.execute("""
        INSERT INTO dm_model_performance (
            user_id, date, predictions_count, 
            true_positives, false_positives, true_negatives, false_negatives,
            precision, recall, f1_score, updated_at
        )
        SELECT 
            p.user_id,
            DATE(p.created_at) as date,
            COUNT(*) as predictions_count,
            COUNT(*) FILTER (WHERE p.sent = TRUE AND r.reaction > 0) as true_positives,
            COUNT(*) FILTER (WHERE p.sent = TRUE AND r.reaction < 0) as false_positives,
            COUNT(*) FILTER (WHERE p.sent = FALSE AND r.reaction IS NULL) as true_negatives,
            COUNT(*) FILTER (WHERE p.sent = FALSE AND r.reaction > 0) as false_negatives,
            -- Precision = TP / (TP + FP)
            CASE 
                WHEN COUNT(*) FILTER (WHERE p.sent = TRUE AND r.reaction IS NOT NULL) > 0
                THEN COUNT(*) FILTER (WHERE p.sent = TRUE AND r.reaction > 0)::FLOAT / 
                     NULLIF(COUNT(*) FILTER (WHERE p.sent = TRUE AND r.reaction IS NOT NULL), 0)
                ELSE 0
            END as precision,
            -- Recall = TP / (TP + FN) - оцениваем на основе реакций
            CASE 
                WHEN COUNT(*) FILTER (WHERE r.reaction > 0) > 0
                THEN COUNT(*) FILTER (WHERE p.sent = TRUE AND r.reaction > 0)::FLOAT / 
                     NULLIF(COUNT(*) FILTER (WHERE r.reaction > 0), 0)
                ELSE 0
            END as recall,
            -- F1 = 2 * (precision * recall) / (precision + recall)
            0 as f1_score,  -- будет пересчитано ниже
            NOW()
        FROM predictions p
        LEFT JOIN reactions r ON p.post_id = r.post_id AND p.user_id = r.user_id
        WHERE DATE(p.created_at) = %s
        GROUP BY p.user_id, DATE(p.created_at)
        ON CONFLICT (user_id, date)
        DO UPDATE SET
            predictions_count = EXCLUDED.predictions_count,
            true_positives = EXCLUDED.true_positives,
            false_positives = EXCLUDED.false_positives,
            true_negatives = EXCLUDED.true_negatives,
            false_negatives = EXCLUDED.false_negatives,
            precision = EXCLUDED.precision,
            recall = EXCLUDED.recall,
            updated_at = NOW()
    """, (target_date,))
    
    # Update F1 score
    cursor.execute("""
        UPDATE dm_model_performance
        SET f1_score = CASE 
            WHEN precision + recall > 0 
            THEN 2 * (precision * recall) / (precision + recall)
            ELSE 0
        END
        WHERE date = %s
    """, (target_date,))
    
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Built dm_model_performance for {target_date}: {affected} rows")


def cleanup_old_data_marts(**context):
    """Remove data mart records older than 90 days."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    tables = ['dm_user_daily_stats', 'dm_tag_stats', 'dm_model_performance']
    
    for table in tables:
        cursor.execute(f"""
            DELETE FROM {table}
            WHERE date < CURRENT_DATE - INTERVAL '90 days'
        """)
        print(f"Cleaned {cursor.rowcount} rows from {table}")
    
    conn.commit()
    cursor.close()
    conn.close()


# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'build_data_marts',
    default_args=default_args,
    description='Build data marts (витрины) for analytics',
    schedule_interval='0 2 * * *',  # Every day at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'data-marts', 'витрины'],
) as dag:
    
    user_stats_task = PythonOperator(
        task_id='build_user_daily_stats',
        python_callable=build_user_daily_stats,
    )
    
    tag_stats_task = PythonOperator(
        task_id='build_tag_stats',
        python_callable=build_tag_stats,
    )
    
    model_perf_task = PythonOperator(
        task_id='build_model_performance',
        python_callable=build_model_performance,
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_old_data_marts',
        python_callable=cleanup_old_data_marts,
    )
    
    # Все витрины строятся параллельно, потом cleanup
    [user_stats_task, tag_stats_task, model_perf_task] >> cleanup_task
