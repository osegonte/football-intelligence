import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to path for imports using absolute paths
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from data_processing.analyzer import FootballDataAnalyzer

st.set_page_config(
    page_title="Football Intelligence Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("⚽ Football Match Intelligence Dashboard")
st.markdown("""
This dashboard provides analytics and insights for upcoming football matches across major leagues.
Data is collected daily from multiple sources including SofaScore and FBref.
""")

# Sidebar filters
st.sidebar.header("Filters")

# Check if data file exists
data_file = os.path.join(parent_dir, "data", "all_matches_latest.csv")
if not os.path.exists(data_file):
    st.error(f"Data file not found: {data_file}")
    st.info("Please run the scraper first to collect match data.")
    st.stop()

# Load data
@st.cache_data
def load_data():
    return pd.read_csv(data_file)

df = load_data()

# Create analyzer
analyzer = FootballDataAnalyzer(data_file)

# Date range filter
df['date'] = pd.to_datetime(df['date'])
min_date = df['date'].min()
max_date = df['date'].max()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, min_date + timedelta(days=7)),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = df[(df['date'].dt.date >= start_date) & 
                     (df['date'].dt.date <= end_date)]
else:
    filtered_df = df

# League filter
all_leagues = sorted(df['league'].unique())
selected_leagues = st.sidebar.multiselect(
    "Select Leagues",
    options=all_leagues,
    default=["Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"]
)

if selected_leagues:
    filtered_df = filtered_df[filtered_df['league'].isin(selected_leagues)]

# Dashboard main content
st.header("Match Overview")

# Match count metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Matches", len(filtered_df))
with col2:
    st.metric("Leagues", filtered_df['league'].nunique())
with col3:
    st.metric("Countries", filtered_df['country'].nunique())

# Upcoming matches
st.subheader("Upcoming Matches")

# Format the match display
def format_match(row):
    return f"{row['home_team']} vs {row['away_team']} | {row['league']} | {row['start_time']}"

# Group by date and display matches
filtered_df = filtered_df.sort_values(['date', 'start_time'])
dates = filtered_df['date'].dt.date.unique()

for date in dates:
    st.write(f"**{date.strftime('%A, %B %d, %Y')}**")
    day_matches = filtered_df[filtered_df['date'].dt.date == date]
    
    for league in day_matches['league'].unique():
        league_matches = day_matches[day_matches['league'] == league]
        st.write(f"*{league}* ({len(league_matches)} matches)")
        
        for _, match in league_matches.iterrows():
            st.write(f"• {match['home_team']} vs {match['away_team']} | {match['start_time']}")
    
    st.write("---")

# Visualizations
st.header("Analytics")

# Matches by league
matches_by_league = analyzer.get_matches_by_league()

if not matches_by_league.empty:
    fig = px.bar(
        matches_by_league.head(10),
        x='league',
        y='count',
        title="Top 10 Leagues by Match Count",
        labels={'league': 'League', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=px.colors.sequential.Blues
    )
    st.plotly_chart(fig, use_container_width=True)

# League distribution by day
league_by_day = analyzer.get_league_distribution_by_day()

if not league_by_day.empty:
    fig = px.imshow(
        league_by_day,
        title="Match Distribution by League and Day",
        labels=dict(x="Day of Week", y="League", color="Match Count"),
        color_continuous_scale="Viridis"
    )
    st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("⚽ Football Intelligence Dashboard | Data refreshed daily")