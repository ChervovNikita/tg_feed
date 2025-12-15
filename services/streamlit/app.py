"""Main Streamlit application."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

from config import settings

# Page config
st.set_page_config(
    page_title="Medium Article Recommender Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_connection():
    """Get database connection."""
    return psycopg2.connect(settings.database_url)


def run_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Run SQL query and return DataFrame."""
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# Sidebar
st.sidebar.title("📊 Medium Recommender")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["📈 Overview", "🔮 Predictions", "👥 Users", "🏷️ Tags", "📊 Metrics"]
)

st.sidebar.markdown("---")
st.sidebar.info("ML-powered Medium article recommendation system")


# Overview Page
if page == "📈 Overview":
    st.title("📈 System Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Total users
    users_count = run_query("SELECT COUNT(*) as count FROM users")
    with col1:
        st.metric("👥 Total Users", users_count['count'].iloc[0] if len(users_count) > 0 else 0)
    
    # Total articles
    articles_count = run_query("SELECT COUNT(*) as count FROM posts")
    with col2:
        st.metric("📄 Total Articles", articles_count['count'].iloc[0] if len(articles_count) > 0 else 0)
    
    # Total reactions
    reactions = run_query("""
        SELECT 
            COUNT(*) FILTER (WHERE reaction > 0) as positive,
            COUNT(*) FILTER (WHERE reaction < 0) as negative
        FROM reactions
        WHERE created_at > NOW() - INTERVAL '24 hours'
    """)
    with col3:
        pos = reactions['positive'].iloc[0] if len(reactions) > 0 else 0
        neg = reactions['negative'].iloc[0] if len(reactions) > 0 else 0
        st.metric("👍 Reactions (24h)", f"{pos} / {neg}")
    
    # Models trained
    models = run_query("SELECT COUNT(*) as count FROM user_models WHERE model_weights IS NOT NULL")
    with col4:
        st.metric("🤖 Trained Models", models['count'].iloc[0] if len(models) > 0 else 0)
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Articles Over Time")
        articles_time = run_query("""
            SELECT 
                date_trunc('day', created_at) as day,
                COUNT(*) as total
            FROM posts
            WHERE created_at > NOW() - INTERVAL '30 days'
            GROUP BY 1
            ORDER BY 1
        """)
        
        if len(articles_time) > 0:
            fig = px.bar(articles_time, x='day', y='total',
                        title='New Articles per Day')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No article data available")
    
    with col2:
        st.subheader("📊 Score Distribution")
        scores = run_query("""
            SELECT score FROM predictions
            WHERE created_at > NOW() - INTERVAL '7 days'
            LIMIT 10000
        """)
        
        if len(scores) > 0:
            fig = px.histogram(scores, x='score', nbins=50,
                              title='Prediction Score Distribution')
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No score data available")
    
    # Engagement metrics
    st.subheader("📈 Engagement Metrics")
    
    engagement = run_query("""
        SELECT 
            date_trunc('day', r.created_at) as day,
            COUNT(*) FILTER (WHERE r.reaction > 0) as likes,
            COUNT(*) FILTER (WHERE r.reaction < 0) as dislikes
        FROM reactions r
        WHERE r.created_at > NOW() - INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 1
    """)
    
    if len(engagement) > 0:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=engagement['day'], y=engagement['likes'], name='Likes', marker_color='#2ecc71'))
        fig.add_trace(go.Bar(x=engagement['day'], y=engagement['dislikes'], name='Dislikes', marker_color='#e74c3c'))
        fig.update_layout(barmode='group', title='Daily Reactions')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No engagement data available")


# Predictions Page
elif page == "🔮 Predictions":
    st.title("🔮 Predictions History")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        days_back = st.selectbox("Time Range", [1, 7, 30], index=1, format_func=lambda x: f"Last {x} days")
    
    with col2:
        sent_filter = st.selectbox("Sent Status", ["All", "Sent", "Not Sent"])
    
    with col3:
        score_min = st.slider("Min Score", 0.0, 1.0, 0.0)
    
    # Build query
    query = """
        SELECT 
            p.id,
            p.user_id,
            p.score,
            p.sent,
            p.created_at,
            po.title,
            po.tag,
            po.author
        FROM predictions p
        JOIN posts po ON p.post_id = po.id
        WHERE p.created_at > NOW() - INTERVAL '%s days'
        AND p.score >= %s
    """
    params = [days_back, score_min]
    
    if sent_filter == "Sent":
        query += " AND p.sent = TRUE"
    elif sent_filter == "Not Sent":
        query += " AND p.sent = FALSE"
    
    query += " ORDER BY p.created_at DESC LIMIT 1000"
    
    predictions = run_query(query, tuple(params))
    
    st.markdown(f"**Found {len(predictions)} predictions**")
    
    if len(predictions) > 0:
        st.dataframe(
            predictions[['id', 'user_id', 'score', 'sent', 'created_at', 'title', 'tag', 'author']],
            use_container_width=True
        )
        
        # Score stats
        st.subheader("Score Statistics")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Mean Score", f"{predictions['score'].mean():.3f}")
        with col2:
            st.metric("Median Score", f"{predictions['score'].median():.3f}")
        with col3:
            st.metric("Std Dev", f"{predictions['score'].std():.3f}")


# Users Page
elif page == "👥 Users":
    st.title("👥 User Analytics")
    
    users = run_query("""
        SELECT 
            u.id,
            u.username,
            u.created_at,
            COUNT(DISTINCT t.tag) as tag_subscriptions,
            COUNT(DISTINCT p.id) as predictions,
            COUNT(DISTINCT r.post_id) as reactions,
            um.accuracy as model_accuracy,
            um.num_samples as model_samples
        FROM users u
        LEFT JOIN tag_subscriptions t ON u.id = t.user_id AND t.is_active = TRUE
        LEFT JOIN predictions p ON u.id = p.user_id
        LEFT JOIN reactions r ON u.id = r.user_id
        LEFT JOIN user_models um ON u.id = um.user_id
        GROUP BY u.id, u.username, u.created_at, um.accuracy, um.num_samples
        ORDER BY predictions DESC
        LIMIT 100
    """)
    
    if len(users) > 0:
        st.dataframe(users, use_container_width=True)
        
        # User stats charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Reactions per User")
            fig = px.bar(users.head(20), x='id', y='reactions',
                        title='Top 20 Users by Reactions')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Model Accuracy Distribution")
            accuracy_data = users[users['model_accuracy'].notna()]
            if len(accuracy_data) > 0:
                fig = px.histogram(accuracy_data, x='model_accuracy', nbins=20,
                                  title='Model Accuracy Distribution')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No trained models yet")
    else:
        st.info("No users found")


# Tags Page
elif page == "🏷️ Tags":
    st.title("🏷️ Tag Analytics")
    
    # Tag popularity
    tag_stats = run_query("""
        SELECT 
            p.tag,
            COUNT(*) as article_count,
            COUNT(DISTINCT t.user_id) as subscribers,
            AVG(pr.score) as avg_score
        FROM posts p
        LEFT JOIN tag_subscriptions t ON p.tag = t.tag AND t.is_active = TRUE
        LEFT JOIN predictions pr ON p.id = pr.post_id
        WHERE p.tag IS NOT NULL
        GROUP BY p.tag
        ORDER BY article_count DESC
    """)
    
    if len(tag_stats) > 0:
        st.subheader("Tag Statistics")
        st.dataframe(tag_stats, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Articles per Tag")
            fig = px.bar(tag_stats.head(15), x='tag', y='article_count',
                        title='Top 15 Tags by Article Count')
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Subscribers per Tag")
            fig = px.bar(tag_stats.head(15), x='tag', y='subscribers',
                        title='Top 15 Tags by Subscribers', color='subscribers')
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Tag performance over time
        st.subheader("Tag Activity Over Time")
        
        tag_activity = run_query("""
            SELECT 
                date_trunc('day', created_at) as day,
                tag,
                COUNT(*) as articles
            FROM posts
            WHERE created_at > NOW() - INTERVAL '30 days'
            AND tag IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1
        """)
        
        if len(tag_activity) > 0:
            # Get top 5 tags
            top_tags = tag_stats.head(5)['tag'].tolist()
            filtered_activity = tag_activity[tag_activity['tag'].isin(top_tags)]
            
            if len(filtered_activity) > 0:
                fig = px.line(filtered_activity, x='day', y='articles', color='tag',
                            title='Articles per Day (Top 5 Tags)')
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No tag data available")


# Metrics Page
elif page == "📊 Metrics":
    st.title("📊 System Metrics")
    
    metrics = run_query("""
        SELECT 
            metric_name,
            metric_value,
            created_at
        FROM metrics
        WHERE created_at > NOW() - INTERVAL '7 days'
        ORDER BY created_at DESC
    """)
    
    if len(metrics) > 0:
        # Get unique metric names
        metric_names = metrics['metric_name'].unique()
        
        selected_metric = st.selectbox("Select Metric", metric_names)
        
        metric_data = metrics[metrics['metric_name'] == selected_metric]
        
        fig = px.line(metric_data, x='created_at', y='metric_value',
                     title=f'{selected_metric} Over Time')
        st.plotly_chart(fig, use_container_width=True)
        
        # Latest values for all metrics
        st.subheader("Latest Metric Values")
        
        latest_metrics = run_query("""
            SELECT DISTINCT ON (metric_name) 
                metric_name,
                metric_value,
                created_at
            FROM metrics
            ORDER BY metric_name, created_at DESC
        """)
        
        if len(latest_metrics) > 0:
            cols = st.columns(min(4, len(latest_metrics)))
            for i, row in latest_metrics.iterrows():
                with cols[i % 4]:
                    st.metric(
                        row['metric_name'],
                        f"{row['metric_value']:.2f}",
                        help=f"Updated: {row['created_at']}"
                    )
    else:
        st.info("No metrics data available. Metrics will appear after Airflow DAGs run.")


# Footer
st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    <div style='text-align: center'>
        <small>Medium Recommender v1.0</small><br>
        <small>MLOps Course Project</small>
    </div>
    """,
    unsafe_allow_html=True
)
