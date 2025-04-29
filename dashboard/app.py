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
data_file = os.path.join(parent_dir, "sofascore_data", "all_matches_latest.csv")
if not os.path.exists(data_file):
    st.error(f"Data file not found: {data_file}")
    st.info("Please run the scraper first to collect match data.")
    
    # Add instructions on how to run the scraper
    st.markdown("""
    ### How to collect match data
    
    Run the scraper using one of the following methods:
    
    **Option 1:** Run the data collection script directly:
    ```bash
    python main.py --days 7 --stats
    ```
    
    **Option 2:** Use the run_all.sh script for a complete setup:
    ```bash
    ./run_all.sh
    ```
    """)
    st.stop()

# Load data
@st.cache_data
def load_data():
    return pd.read_csv(data_file)

df = load_data()

# Create analyzer
analyzer = FootballDataAnalyzer(data_file)

# Date range filter\
# Date range filter
# Date range filter
df['date'] = pd.to_datetime(df['date'])
min_date = df['date'].min().date()  # Convert to date object
max_date = df['date'].max().date()  # Convert to date object

# Calculate default end date within available range
default_end_date = min(min_date + timedelta(days=3), max_date)

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, default_end_date),  # Use the calculated default_end_date
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
    default=["Premier League", "LaLiga", "Bundesliga", "Serie A", "Ligue 1"]
)

if selected_leagues:
    filtered_df = filtered_df[filtered_df['league'].isin(selected_leagues)]

# Country filter
all_countries = sorted(df['country'].unique())
selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=all_countries,
    default=[]
)

if selected_countries:
    filtered_df = filtered_df[filtered_df['country'].isin(selected_countries)]

# Status filter
all_statuses = sorted(df['status'].unique())
selected_statuses = st.sidebar.multiselect(
    "Select Match Status",
    options=all_statuses,
    default=[]
)

if selected_statuses:
    filtered_df = filtered_df[filtered_df['status'].isin(selected_statuses)]

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

# Visualizations
st.header("Analytics")

# Create tabs for different visualizations
tab1, tab2, tab3, tab4 = st.tabs(["Leagues", "Countries", "Match Calendar", "Teams"])

# Tab 1: Leagues
with tab1:
    # Matches by league
    st.subheader("Top 10 Leagues by Match Count")
    matches_by_league = analyzer.get_matches_by_league()
    
    if not matches_by_league.empty:
        top_leagues = matches_by_league.head(10)
        fig = px.bar(
            top_leagues,
            x='league',
            y='count',
            title="Top 10 Leagues by Match Count",
            labels={'league': 'League', 'count': 'Number of Matches'},
            color='count',
            color_continuous_scale=px.colors.sequential.Blues
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # League distribution by day
    st.subheader("Match Distribution by League and Day")
    league_by_day = analyzer.get_league_distribution_by_day()
    
    if not league_by_day.empty:
        fig = px.imshow(
            league_by_day,
            title="Match Distribution by League and Day",
            labels=dict(x="Day of Week", y="League", color="Match Count"),
            color_continuous_scale="Viridis"
        )
        st.plotly_chart(fig, use_container_width=True)

# Tab 2: Countries
with tab2:
    # Matches by country
    st.subheader("Top 10 Countries by Match Count")
    country_counts = filtered_df['country'].value_counts().reset_index()
    country_counts.columns = ['country', 'count']
    
    top_countries = country_counts.head(10)
    fig = px.bar(
        top_countries,
        x='country',
        y='count',
        title="Top 10 Countries by Match Count",
        labels={'country': 'Country', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=px.colors.sequential.Reds
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # World map of matches
    st.subheader("Global Match Distribution")
    # Note: This would require country codes for a proper map
    # For now we'll just display a table of all countries
    all_country_counts = country_counts.sort_values(by='count', ascending=False)
    st.dataframe(all_country_counts)

# Tab 3: Match Calendar
with tab3:
    st.subheader("Match Calendar")
    
    # Create a data frame for calendar view
    calendar_df = filtered_df.copy()
    calendar_df['day'] = calendar_df['date'].dt.day_name()
    calendar_df['week'] = calendar_df['date'].dt.isocalendar().week
    
    # Create pivot table
    pivot = calendar_df.pivot_table(
        index='week', 
        columns='day',
        values='id', 
        aggfunc='count', 
        fill_value=0
    )
    
    # Create heatmap
    fig = px.imshow(
        pivot,
        title="Match Calendar Heatmap",
        labels=dict(x="Day of Week", y="Week", color="Match Count"),
        color_continuous_scale="Viridis"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Match count by date
    dates_df = filtered_df['date'].dt.date.value_counts().reset_index()
    dates_df.columns = ['date', 'matches']
    dates_df = dates_df.sort_values('date')
    
    fig = px.line(
        dates_df,
        x='date',
        y='matches',
        title="Matches per Day",
        labels={'date': 'Date', 'matches': 'Number of Matches'},
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)

# Tab 4: Teams
with tab4:
    st.subheader("Team Analysis")
    
    # Count home and away appearances
    home_teams = filtered_df['home_team'].value_counts()
    away_teams = filtered_df['away_team'].value_counts()
    
    # Combine counts
    all_teams = home_teams.add(away_teams, fill_value=0).sort_values(ascending=False)
    all_teams = all_teams.head(20).reset_index()
    all_teams.columns = ['team', 'appearances']
    
    fig = px.bar(
        all_teams,
        y='team',
        x='appearances',
        orientation='h',
        title="Top 20 Teams by Match Appearances",
        labels={'appearances': 'Number of Matches', 'team': 'Team'},
        color='appearances',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

# Upcoming matches
st.header("Upcoming Matches")

# Add a search box for teams
search_team = st.text_input("Search for a team:")

# Format the match display
def format_match(row):
    return f"{row['home_team']} vs {row['away_team']} | {row['league']} | {row['start_time']}"

# Apply search filter if provided
if search_team:
    search_term = search_team.lower()
    filtered_df = filtered_df[
        filtered_df['home_team'].str.lower().str.contains(search_term) | 
        filtered_df['away_team'].str.lower().str.contains(search_term)
    ]

# Group by date and display matches
filtered_df = filtered_df.sort_values(['date', 'start_time'])
dates = filtered_df['date'].dt.date.unique()

for date in dates:
    st.write(f"**{date.strftime('%A, %B %d, %Y')}**")
    day_matches = filtered_df[filtered_df['date'].dt.date == date]
    
    # Group by league
    leagues = day_matches['league'].unique()
    
    for league in leagues:
        league_matches = day_matches[day_matches['league'] == league]
        with st.expander(f"{league} ({len(league_matches)} matches)"):
            for _, match in league_matches.iterrows():
                status_color = ""
                if match['status'] == 'Ended':
                    status_color = "gray"
                elif match['status'] == 'Not started':
                    status_color = "blue"
                else:
                    status_color = "green"
                
                st.markdown(
                    f"• **{match['home_team']}** vs **{match['away_team']}** | "
                    f"{match['start_time']} | "
                    f"<span style='color:{status_color}'>{match['status']}</span>", 
                    unsafe_allow_html=True
                )
    
    st.write("---")

# Footer
st.markdown("---")
st.markdown("⚽ Football Intelligence Dashboard | Data refreshed daily")