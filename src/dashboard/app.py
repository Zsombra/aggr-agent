import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from modules.liquidation_heatmap import LiquidationHeatmap
from modules.funding_monitor import FundingMonitor

st.set_page_config(page_title="MAEZTRO Dashboard", page_icon="📊", layout="wide")
st.title("🎯 MAEZTRO Trading Dashboard")

tab1, tab2 = st.tabs(["Liquidation Heatmap", "Funding Monitor"])

with tab1:
    col1, col2, col3 = st.columns(3)
    with col1:
        symbol = st.selectbox("Symbol", ["BTCUSDT", "ETHUSDT", "SOLUSDT"], key="liq_symbol")
    with col2:
        hours = st.slider("Hours Back", 1, 72, 24, key="liq_hours")
    with col3:
        if st.button("Refresh", key="liq_refresh"):
            st.rerun()
    
    heatmap = LiquidationHeatmap(symbol)
    current_price = heatmap.get_current_price()
    summary = heatmap.get_summary(hours)
    data = heatmap.generate_heatmap_data(hours)
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Price", f"${current_price:,.0f}")
    c2.metric("Total Liqs", f"{summary['total_count']}")
    c3.metric("Longs Rekt", f"{summary['longs']}")
    c4.metric("Shorts Rekt", f"{summary['shorts']}")
    c5.metric("Bias", summary['bias'])
    
    if not data.empty:
        fig = make_subplots(rows=1, cols=2, subplot_titles=("By Count", "By USD"))
        data_sorted = data.sort_values('bucket')
        labels = [f"${b:,.0f}" for b in data_sorted['bucket']]
        
        fig.add_trace(go.Bar(y=labels, x=-data_sorted['longs'], orientation='h', name='Longs', marker_color='#ef4444'), row=1, col=1)
        fig.add_trace(go.Bar(y=labels, x=data_sorted['shorts'], orientation='h', name='Shorts', marker_color='#22c55e'), row=1, col=1)
        fig.add_trace(go.Bar(y=labels, x=-data_sorted['long_usd']/1000, orientation='h', name='Long $', marker_color='#f87171'), row=1, col=2)
        fig.add_trace(go.Bar(y=labels, x=data_sorted['short_usd']/1000, orientation='h', name='Short $', marker_color='#4ade80'), row=1, col=2)
        
        fig.update_layout(height=500, template='plotly_dark', barmode='overlay')
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Top Gravity Zones")
        st.dataframe(data[['price_range','count','usd_total','longs','shorts','gravity']].head(10), hide_index=True)
    else:
        st.warning("No liquidation data")

with tab2:
    col1, col2, col3 = st.columns(3)
    with col1:
        f_symbol = st.selectbox("Symbol", ["BTCUSDT", "ETHUSDT", "SOLUSDT"], key="fund_symbol")
    with col2:
        f_hours = st.slider("Hours Back", 1, 72, 24, key="fund_hours")
    with col3:
        if st.button("Refresh", key="fund_refresh"):
            st.rerun()
    
    fm = FundingMonitor(f_symbol)
    df = fm.fetch_history(f_hours)
    stats = fm.calculate_stats(df)
    
    if stats:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Current Rate", f"{stats['current_rate']*100:.4f}%")
        c2.metric("APR", f"{stats['apr']:.1f}%")
        c3.metric("Avg Rate", f"{stats['avg_rate']*100:.4f}%")
        c4.metric("Trend", stats['trend'])
        
        fig = fm.create_chart(df)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No funding data")
