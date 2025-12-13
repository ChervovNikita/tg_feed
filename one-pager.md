# TG Channel Filter — ML-powered Post Filtering

## 🎯 Бизнес-задача

**Проблема**: Информационная перегрузка — пользователи читают лишь 10-20% постов из каналов.

**Решение**: Telegram бот, который автоматически фильтрует посты на основе предпочтений пользователя, обучаясь на его реакциях.

---

## 🏗️ Архитектура

```
Каналы → Userbot → Kafka → ML Service → Kafka → Bot → Пользователь
                              ↓                    ↓
                         PostgreSQL ← Reactions ←──┘
                              ↓
                          Airflow (retrain)
```

**Компоненты**: Pyrogram Userbot | aiogram Bot | FastAPI ML | PostgreSQL + pgvector | Kafka | Airflow | Streamlit | Grafana

---

## 🔬 ML Pipeline

| Этап | Технология |
|------|------------|
| Эмбеддинги | OpenAI text-embedding-3-small (1536 dims) |
| Модель | LogisticRegression (per-user) |
| Обучение | Airflow DAG каждые 6 часов |
| Инференс | Real-time через Kafka consumers |

---

## 📊 Метрики

| Метрика | Источник |
|---------|----------|
| predictions_total | Prometheus |
| inference_latency | Prometheus |
| precision/recall | PostgreSQL → Grafana |
| engagement_rate | Airflow → Streamlit |

---

## 🚀 Запуск

```bash
git clone https://github.com/your-username/tg_channel_filter.git
cd tg_channel_filter
cp .env.example .env  # заполнить credentials
docker-compose up -d
```

**Порты**: ML API :8000 | Streamlit :8501 | Grafana :3000 | Airflow :8080

---

## 📦 Стек технологий

- **Backend**: Python 3.11, FastAPI, aiogram 3, Pyrogram
- **ML**: scikit-learn, OpenAI API
- **Data**: PostgreSQL 16 + pgvector, Kafka
- **Ops**: Docker Compose, Airflow, Prometheus, Grafana
- **UI**: Streamlit, Telegram Bot

---

## ✅ Покрытие требований

- [x] Docker-compose (все сервисы)
- [x] ML-инференс через брокер
- [x] ETL/оркестратор (Airflow)
- [x] Переобучение без остановки
- [x] Продвинутый UI + OpenAPI
- [x] Мониторинг (Grafana + Prometheus)

---

**Автор**: MLOps Course Final Project | **Дедлайн**: 31.12.2025

