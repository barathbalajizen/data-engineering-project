"""
E-Commerce Sales & Sentiment Analytics Dashboard

Interactive Streamlit dashboard for real-time pipeline insights:
- Revenue trends by date/category
- Top/Bottom performing products by sales and sentiment
- Customer sentiment distribution
- Flagged negative reviews requiring attention
"""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Page configuration
st.set_page_config(
    page_title="E-Commerce Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .positive {
        color: #28a745;
        font-weight: bold;
    }
    .negative {
        color: #dc3545;
        font-weight: bold;
    }
    .neutral {
        color: #6c757d;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_gold_tables():
    """Load Gold layer tables from Delta Lake."""
    from deltalake import DeltaTable
    
    base_path = os.getenv('LOCAL_DATA_LAKE_PATH', 'C:/ecommerce_delta_lake')
    
    try:
        # Load Gold tables
        fact_orders = pl.from_arrow(
            DeltaTable(f"{base_path}/gold/fact_orders/").to_pyarrow_table()
        )
        dim_product = pl.from_arrow(
            DeltaTable(f"{base_path}/gold/dim_product/").to_pyarrow_table()
        )
        dim_customer = pl.from_arrow(
            DeltaTable(f"{base_path}/gold/dim_customer/").to_pyarrow_table()
        )
        dim_date = pl.from_arrow(
            DeltaTable(f"{base_path}/gold/dim_date/").to_pyarrow_table()
        )
        
        return {
            'orders': fact_orders,
            'products': dim_product,
            'customers': dim_customer,
            'dates': dim_date
        }
    except Exception as e:
        logger.error(f"Failed to load Gold tables: {str(e)}")
        return None


def create_sample_data():
    """Create sample data for demo when Delta tables unavailable."""
    # Sample orders
    orders = pl.DataFrame({
        'order_id': ['O001', 'O002', 'O003', 'O004', 'O005'],
        'product_id': ['P001', 'P002', 'P001', 'P003', 'P002'],
        'customer_id': ['C001', 'C002', 'C001', 'C001', 'C002'],
        'order_date': [
            '2024-01-10', '2024-01-11', '2024-01-12', '2024-01-13', '2024-01-14'
        ],
        'total_amount': [1049.99, 69.98, 1049.99, 339.99, 104.97],
        'sentiment_score': [0.8, 0.3, 0.9, -0.8, 0.1],
        'sentiment_label': ['positive', 'neutral', 'positive', 'negative', 'neutral']
    })
    
    # Sample products
    products = pl.DataFrame({
        'product_id': ['P001', 'P002', 'P003'],
        'product_name': ['Laptop', 'Mouse', 'Monitor'],
        'category': ['Electronics', 'Accessories', 'Electronics'],
        'price': [999.99, 29.99, 299.99]
    })
    
    return {'orders': orders, 'products': products}


def format_currency(value):
    """Format value as currency."""
    return f"${value:,.2f}"


def main():
    # Header
    st.title("📊 E-Commerce Sales & Sentiment Dashboard")
    st.markdown("Real-time pipeline insights from Bronze → Silver → Gold layers")
    
    # Load data
    with st.spinner("Loading data from Gold layer..."):
        data = load_gold_tables()
    
    if data is None:
        st.warning("⚠️ Cannot connect to Gold layer. Using sample data for demo.")
        data = create_sample_data()
        demo_mode = True
    else:
        demo_mode = False
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    orders_df = data['orders']
    
    # Date range filter
    if 'order_date' in orders_df.columns:
        # Handle date type conversions
        if orders_df['order_date'].dtype == pl.Utf8:
            orders_df = orders_df.with_columns(
                pl.col('order_date').cast(pl.Date)
            )
        elif orders_df['order_date'].dtype == pl.Datetime:
            orders_df = orders_df.with_columns(
                pl.col('order_date').cast(pl.Date)
            )
        
        min_date = orders_df['order_date'].min()
        max_date = orders_df['order_date'].max()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            orders_df = orders_df.filter(
                (pl.col('order_date') >= date_range[0]) &
                (pl.col('order_date') <= date_range[1])
            )
    
    # Sentiment filter
    if 'sentiment_label' in orders_df.columns:
        sentiment_filter = st.sidebar.multiselect(
            "Sentiment Filter",
            options=['positive', 'neutral', 'negative'],
            default=['positive', 'neutral', 'negative']
        )
        orders_df = orders_df.filter(pl.col('sentiment_label').is_in(sentiment_filter))
    
    # Key Metrics
    st.header("📈 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = orders_df['total_amount'].sum()
        st.metric("Total Revenue", format_currency(total_revenue))
    
    with col2:
        order_count = len(orders_df)
        st.metric("Total Orders", f"{order_count:,}")
    
    with col3:
        avg_order_value = orders_df['total_amount'].mean() if order_count > 0 else 0
        st.metric("Avg Order Value", format_currency(avg_order_value))
    
    with col4:
        if 'sentiment_label' in orders_df.columns:
            positive_count = len(
                orders_df.filter(pl.col('sentiment_label') == 'positive')
            )
            positive_pct = (positive_count / order_count * 100) if order_count > 0 else 0
            st.metric("Positive Reviews", f"{positive_pct:.1f}%")
    
    # Revenue Trend
    st.header("💰 Revenue Trend")
    if 'order_date' in orders_df.columns:
        revenue_by_date = orders_df.group_by('order_date').agg(
            pl.col('total_amount').sum().alias('revenue')
        ).sort('order_date')
        
        fig = px.line(
            revenue_by_date,
            x='order_date',
            y='revenue',
            markers=True,
            title="Daily Revenue",
            labels={'order_date': 'Date', 'revenue': 'Revenue ($)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Product Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("🏆 Top Products by Revenue")
        if 'product_id' in orders_df.columns and 'products' in data:
            top_products = (
                orders_df
                .group_by('product_id')
                .agg(pl.col('total_amount').sum().alias('revenue'))
                .sort('revenue', descending=True)
                .head(5)
            )
            
            # Join with product names
            if 'product_name' in data['products'].columns:
                top_products = top_products.join(
                    data['products'].select(['product_id', 'product_name']),
                    on='product_id'
                )
            
            fig = px.bar(
                top_products,
                x='revenue',
                y='product_id' if 'product_name' not in top_products.columns else 'product_name',
                orientation='h',
                title="Top 5 Products by Revenue",
                labels={'product_id': 'Product', 'revenue': 'Revenue ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.header("😞 Bottom Products by Sentiment")
        if 'product_id' in orders_df.columns and 'sentiment_score' in orders_df.columns:
            worst_products = (
                orders_df
                .group_by('product_id')
                .agg(pl.col('sentiment_score').mean().alias('avg_sentiment'))
                .sort('avg_sentiment', ascending=True)
                .head(5)
            )
            
            # Join with product names
            if 'product_name' in data['products'].columns:
                worst_products = worst_products.join(
                    data['products'].select(['product_id', 'product_name']),
                    on='product_id'
                )
            
            fig = px.bar(
                worst_products,
                x='avg_sentiment',
                y='product_id' if 'product_name' not in worst_products.columns else 'product_name',
                orientation='h',
                title="Bottom 5 Products by Sentiment",
                labels={'product_id': 'Product', 'avg_sentiment': 'Avg Sentiment'},
                color='avg_sentiment',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Sentiment Distribution
    st.header("😊 Sentiment Distribution")
    if 'sentiment_label' in orders_df.columns:
        col1, col2 = st.columns([1, 1])
        
        with col1:
            sentiment_counts = (
                orders_df
                .group_by('sentiment_label')
                .agg(pl.count('sentiment_label').alias('count'))
            )
            
            fig = px.pie(
                sentiment_counts,
                values='count',
                names='sentiment_label',
                title="Reviews by Sentiment",
                color_discrete_map={
                    'positive': '#28a745',
                    'neutral': '#6c757d',
                    'negative': '#dc3545'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'sentiment_score' in orders_df.columns:
                fig = px.histogram(
                    orders_df.select('sentiment_score'),
                    x='sentiment_score',
                    nbins=20,
                    title="Sentiment Score Distribution",
                    labels={'sentiment_score': 'Sentiment Score'},
                    color_discrete_sequence=['#3498db']
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Flagged Reviews
    st.header("⚠️ Flagged Negative Reviews")
    if 'sentiment_label' in orders_df.columns:
        negative_reviews = orders_df.filter(
            pl.col('sentiment_label') == 'negative'
        )
        
        if len(negative_reviews) > 0:
            st.warning(f"Found {len(negative_reviews)} negative reviews requiring attention")
            
            # Display as table
            display_cols = []
            if 'order_id' in negative_reviews.columns:
                display_cols.append('order_id')
            if 'product_id' in negative_reviews.columns:
                display_cols.append('product_id')
            if 'sentiment_score' in negative_reviews.columns:
                display_cols.append('sentiment_score')
            
            if display_cols:
                st.dataframe(
                    negative_reviews.select(display_cols).head(20),
                    use_container_width=True
                )
        else:
            st.success("✅ No negative reviews in selected period")
    
    # Footer
    st.divider()
    st.markdown("""
    ---
    **Dashboard Info**
    - Data source: Gold layer (Delta Lake)
    - Updated: Every pipeline run
    - Demo mode: {} | Contact: [your-email]
    """.format("✅ Active" if not demo_mode else "⚠️ Sample Data"))


if __name__ == "__main__":
    main()
