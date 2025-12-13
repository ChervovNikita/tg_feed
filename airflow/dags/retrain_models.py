"""DAG for retraining user models via ML Service API."""
import os
from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# Configuration
ML_SERVICE_URL = os.environ.get('ML_SERVICE_URL', 'http://ml-service:8000')


def get_users_for_training(**context):
    """Get list of users who need model training from ML Service."""
    try:
        response = requests.get(
            f"{ML_SERVICE_URL}/users/trainable",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        user_ids = data.get('users', [])
        print(f"Found {len(user_ids)} users eligible for training")
        
        return user_ids
    except Exception as e:
        print(f"Error getting trainable users: {e}")
        return []


def train_user_model_via_api(user_id: int) -> dict:
    """Train model for a single user via ML Service API."""
    try:
        response = requests.post(
            f"{ML_SERVICE_URL}/retrain/{user_id}",
            timeout=120  # Training can take time
        )
        response.raise_for_status()
        result = response.json()
        
        return {
            'user_id': user_id,
            'success': result.get('success', False),
            'accuracy': result.get('accuracy'),
            'num_samples': result.get('num_samples', 0),
            'message': result.get('message', '')
        }
    except requests.exceptions.Timeout:
        return {'user_id': user_id, 'success': False, 'reason': 'Timeout'}
    except Exception as e:
        return {'user_id': user_id, 'success': False, 'reason': str(e)}


def train_all_models(**context):
    """Train models for all eligible users via ML Service API."""
    ti = context['ti']
    user_ids = ti.xcom_pull(task_ids='get_users')
    
    if not user_ids:
        print("No users to train")
        return []
    
    results = []
    for user_id in user_ids:
        result = train_user_model_via_api(user_id)
        results.append(result)
        
        if result.get('success'):
            print(f"User {user_id}: trained successfully, accuracy={result.get('accuracy'):.3f}")
        else:
            print(f"User {user_id}: failed - {result.get('reason', result.get('message', 'unknown'))}")
    
    successful = sum(1 for r in results if r.get('success'))
    print(f"Training complete: {successful}/{len(results)} models trained successfully")
    
    return results


def log_training_summary(**context):
    """Log training summary."""
    ti = context['ti']
    results = ti.xcom_pull(task_ids='train_models')
    
    if not results:
        print("No training results to log")
        return
    
    successful = sum(1 for r in results if r.get('success'))
    accuracies = [r['accuracy'] for r in results if r.get('success') and r.get('accuracy')]
    avg_accuracy = sum(accuracies) / len(accuracies) if accuracies else 0
    
    print("=" * 50)
    print("TRAINING SUMMARY")
    print("=" * 50)
    print(f"Total users processed: {len(results)}")
    print(f"Successfully trained: {successful}")
    print(f"Failed: {len(results) - successful}")
    print(f"Average accuracy: {avg_accuracy:.3f}")
    print("=" * 50)


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
    'retrain_user_models',
    default_args=default_args,
    description='Retrain user ML models via ML Service API',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'training'],
) as dag:
    
    get_users_task = PythonOperator(
        task_id='get_users',
        python_callable=get_users_for_training,
    )
    
    train_models_task = PythonOperator(
        task_id='train_models',
        python_callable=train_all_models,
    )
    
    log_summary_task = PythonOperator(
        task_id='log_summary',
        python_callable=log_training_summary,
    )
    
    get_users_task >> train_models_task >> log_summary_task
