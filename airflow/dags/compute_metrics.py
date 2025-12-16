"""DAG for computing business metrics."""
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


def compute_prediction_metrics(**context):
    """Compute prediction-related metrics."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get hourly prediction stats
    cursor.execute("""
        SELECT 
            COUNT(*) as total_predictions,
            COUNT(*) FILTER (WHERE sent = TRUE) as sent_predictions,
            AVG(score) as avg_score
        FROM predictions
        WHERE created_at > NOW() - INTERVAL '1 hour'
    """)
    
    stats = cursor.fetchone()
    
    # Get reaction stats
    cursor.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE reaction > 0) as positive_reactions,
            COUNT(*) FILTER (WHERE reaction < 0) as negative_reactions
        FROM reactions
        WHERE created_at > NOW() - INTERVAL '1 hour'
    """)
    
    reactions = cursor.fetchone()
    
    # Compute precision (if we have reactions for sent posts)
    cursor.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE r.reaction > 0) as true_positives,
            COUNT(*) FILTER (WHERE r.reaction < 0) as false_positives
        FROM predictions p
        JOIN reactions r ON p.post_id = r.post_id AND p.user_id = r.user_id
        WHERE p.sent = TRUE
        AND p.created_at > NOW() - INTERVAL '24 hours'
    """)
    
    precision_data = cursor.fetchone()
    tp = precision_data['true_positives'] or 0
    fp = precision_data['false_positives'] or 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    
    # Save metrics
    metrics = [
        ('hourly_predictions', stats['total_predictions'] or 0),
        ('hourly_sent_predictions', stats['sent_predictions'] or 0),
        ('hourly_avg_score', stats['avg_score'] or 0),
        ('hourly_positive_reactions', reactions['positive_reactions'] or 0),
        ('hourly_negative_reactions', reactions['negative_reactions'] or 0),
        ('daily_precision', precision),
    ]
    
    for metric_name, metric_value in metrics:
        cursor.execute("""
            INSERT INTO metrics (metric_name, metric_value, labels, created_at)
            VALUES (%s, %s, '{"dag": "compute_metrics"}'::jsonb, NOW())
        """, (metric_name, metric_value))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Computed metrics: {metrics}")


def compute_user_metrics(**context):
    """Compute user-related metrics."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Active users (with predictions in last 24h)
    cursor.execute("""
        SELECT COUNT(DISTINCT user_id) as active_users
        FROM predictions
        WHERE created_at > NOW() - INTERVAL '24 hours'
    """)
    
    active = cursor.fetchone()
    
    # Users with models
    cursor.execute("""
        SELECT COUNT(*) as users_with_models
        FROM user_models
        WHERE model_weights IS NOT NULL
    """)
    
    models = cursor.fetchone()
    
    # Total subscriptions (for Medium version: tag_subscriptions)
    cursor.execute("""
        SELECT COUNT(*) as total_subscriptions
        FROM tag_subscriptions
        WHERE is_active = TRUE
    """)
    subs = cursor.fetchone()
    
    # Engagement rate (reactions / sent posts)
    cursor.execute("""
        WITH sent AS (
            SELECT COUNT(*) as cnt FROM predictions 
            WHERE sent = TRUE AND created_at > NOW() - INTERVAL '24 hours'
        ),
        reacted AS (
            SELECT COUNT(*) as cnt FROM reactions 
            WHERE created_at > NOW() - INTERVAL '24 hours'
        )
        SELECT 
            sent.cnt as sent_count,
            reacted.cnt as reaction_count
        FROM sent, reacted
    """)
    
    engagement = cursor.fetchone()
    sent_count = engagement['sent_count'] or 0
    reaction_count = engagement['reaction_count'] or 0
    engagement_rate = reaction_count / sent_count if sent_count > 0 else 0
    
    # Save metrics
    metrics = [
        ('daily_active_users', active['active_users'] or 0),
        ('users_with_models', models['users_with_models'] or 0),
        ('total_subscriptions', subs['total_subscriptions'] or 0),
        ('engagement_rate', engagement_rate),
    ]
    
    for metric_name, metric_value in metrics:
        cursor.execute("""
            INSERT INTO metrics (metric_name, metric_value, labels, created_at)
            VALUES (%s, %s, '{"dag": "compute_metrics"}'::jsonb, NOW())
        """, (metric_name, metric_value))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Computed user metrics: {metrics}")


def cleanup_old_metrics(**context):
    """Remove metrics older than 30 days."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        DELETE FROM metrics
        WHERE created_at < NOW() - INTERVAL '30 days'
    """)
    
    deleted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Cleaned up {deleted} old metric records")


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
    'compute_metrics',
    default_args=default_args,
    description='Compute business metrics for monitoring',
    schedule_interval='0 * * * *',  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['metrics', 'monitoring'],
) as dag:
    
    prediction_metrics_task = PythonOperator(
        task_id='compute_prediction_metrics',
        python_callable=compute_prediction_metrics,
    )
    
    user_metrics_task = PythonOperator(
        task_id='compute_user_metrics',
        python_callable=compute_user_metrics,
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_old_metrics',
        python_callable=cleanup_old_metrics,
    )
    
    [prediction_metrics_task, user_metrics_task] >> cleanup_task

