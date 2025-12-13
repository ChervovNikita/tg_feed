"""Prometheus metrics for ML Service."""
from prometheus_client import Counter, Histogram, Gauge

# Prediction metrics
predictions_total = Counter(
    'predictions_total',
    'Total number of predictions made'
)

predictions_sent_total = Counter(
    'predictions_sent_total',
    'Total number of posts sent to users'
)

# Reaction metrics
reactions_total = Counter(
    'reactions_total',
    'Total number of reactions received',
    ['reaction']  # positive, negative, mute
)

# Latency metrics
inference_latency = Histogram(
    'inference_latency_seconds',
    'Time spent on inference',
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

embedding_latency = Histogram(
    'embedding_latency_seconds',
    'Time spent on embedding generation',
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# Model metrics
models_trained_total = Counter(
    'models_trained_total',
    'Total number of models trained'
)

active_users = Gauge(
    'active_users',
    'Number of users with trained models'
)

