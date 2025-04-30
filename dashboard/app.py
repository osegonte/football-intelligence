import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Add parent directory to path for imports using absolute paths
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from data_processing.analyzer import FootballDataAnalyzer

# Set page configuration
st.set_page_config(
    page_title="Football Intelligence Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom CSS
st.markdown("""
<style>
    .main {
        background-color: #f9f9f9;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f0f2f6;
        border-radius: 4px 4px 0 0;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4f8bf9;
        color: white;
    }
    .metric-container {
        background-color: white;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .match-card {
        background-color: white;
        border-radius: 8px;
        padding: 10px;
        margin-bottom: 10px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .league-header {
        background-color: #e6e6e6;
        padding: 5px 10px;
        border-radius: 5px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Title and description
st.title("⚽ Football Match Intelligence Dashboard")
st.markdown("""
This dashboard provides analytics and insights for upcoming football matches across major leagues.
Data is collected daily from multiple sources including SofaScore and FBref.
""")

# Check if data file exists
data_file = os.path.join(parent_dir, "data", "all_matches_latest.csv")
if not os.path.exists(data_file):
    data_file = os.path.join(parent_dir, "sofascore_data", "all_matches_latest.csv")

if not os.path.exists(data_file):
    st.error(f"Data file not found. Please run the scraper first to collect match data.")
    st.info("You can run the scraper with: python main.py --days 7 --stats")
    st.stop()

# Load data
@st.cache_data
def load_data():
    df = pd.read_csv(data_file)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    return df

df = load_data()

# Create analyzer
analyzer = FootballDataAnalyzer(data_file)

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
min_date = df['date'].min().date()
max_date = df['date'].max().date()

# Ensure the default end date doesn't exceed the maximum date in the dataset
default_end_date = min(min_date + timedelta(days=7), max_date)

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, default_end_date),
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
default_leagues = ["Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"]
# Ensure default leagues actually exist in the data
default_leagues = [league for league in default_leagues if league in all_leagues]

selected_leagues = st.sidebar.multiselect(
    "Select Leagues",
    options=all_leagues,
    default=default_leagues if default_leagues else all_leagues[:5]
)

if selected_leagues:
    filtered_df = filtered_df[filtered_df['league'].isin(selected_leagues)]

# Country filter
all_countries = sorted(filtered_df['country'].unique())
selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=all_countries,
    default=[]
)

if selected_countries:
    filtered_df = filtered_df[filtered_df['country'].isin(selected_countries)]

# Team filter
all_teams = set()
for team in filtered_df['home_team'].unique():
    all_teams.add(team)
for team in filtered_df['away_team'].unique():
    all_teams.add(team)
all_teams = sorted(list(all_teams))

selected_team = st.sidebar.selectbox(
    "Filter by Team",
    options=["All Teams"] + all_teams,
    index=0
)

if selected_team != "All Teams":
    filtered_df = filtered_df[(filtered_df['home_team'] == selected_team) | 
                              (filtered_df['away_team'] == selected_team)]

# Create tabs for different dashboard sections
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📆 Schedule", "📈 Analytics", "🔍 Teams"])

with tab1:
    st.header("Match Overview")
    
    # Match count metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        with st.container():
            st.markdown('<div class="metric-container">', unsafe_allow_html=True)
            st.metric("Total Matches", len(filtered_df))
            st.markdown('</div>', unsafe_allow_html=True)
    with col2:
        with st.container():
            st.markdown('<div class="metric-container">', unsafe_allow_html=True)
            st.metric("Leagues", filtered_df['league'].nunique())
            st.markdown('</div>', unsafe_allow_html=True)
    with col3:
        with st.container():
            st.markdown('<div class="metric-container">', unsafe_allow_html=True)
            st.metric("Countries", filtered_df['country'].nunique())
            st.markdown('</div>', unsafe_allow_html=True)
    with col4:
        with st.container():
            st.markdown('<div class="metric-container">', unsafe_allow_html=True)
            days_count = filtered_df['date'].dt.date.nunique()
            st.metric("Days", days_count)
            st.markdown('</div>', unsafe_allow_html=True)
    
    st.subheader("Matches by League")
    
    # Get match counts by league
    league_counts = filtered_df['league'].value_counts().reset_index()
    league_counts.columns = ['league', 'count']
    
    # Show top N leagues
    top_n = min(10, len(league_counts))
    top_leagues = league_counts.head(top_n)
    
    # Create bar chart
    fig = px.bar(
        top_leagues,
        x='league',
        y='count',
        title=f"Top {top_n} Leagues by Match Count",
        labels={'league': 'League', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=px.colors.sequential.Blues
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Matches by country
    st.subheader("Matches by Country")
    country_counts = filtered_df['country'].value_counts().reset_index()
    country_counts.columns = ['country', 'count']
    
    # Show top N countries
    top_n = min(10, len(country_counts))
    top_countries = country_counts.head(top_n)
    
    # Create bar chart
    fig = px.bar(
        top_countries,
        x='country',
        y='count',
        title=f"Top {top_n} Countries by Match Count",
        labels={'country': 'Country', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header("Match Schedule")
    
    # Group by date and display matches
    filtered_df = filtered_df.sort_values(['date', 'start_time'])
    dates = filtered_df['date'].dt.date.unique()
    
    for date in dates:
        st.subheader(f"{date.strftime('%A, %B %d, %Y')}")
        day_matches = filtered_df[filtered_df['date'].dt.date == date]
        
        for league in sorted(day_matches['league'].unique()):
            league_matches = day_matches[day_matches['league'] == league]
            with st.expander(f"{league} ({len(league_matches)} matches)", expanded=True):
                for _, match in league_matches.iterrows():
                    st.markdown(f"""
                    <div class="match-card">
                        <strong>{match['home_team']}</strong> vs <strong>{match['away_team']}</strong>
                        <span style="float:right">{match['start_time']}</span>
                        <p><small>{match['country']} • {match['league']}</small></p>
                    </div>
                    """, unsafe_allow_html=True)

with tab3:
    st.header("Advanced Analytics")
    
    # Team appearances (combined home and away)
    st.subheader("Team Appearances")
    
    # Create a function to count team appearances
    def get_team_appearances(df):
        home_teams = df['home_team'].value_counts()
        away_teams = df['away_team'].value_counts()
        
        all_teams = home_teams.add(away_teams, fill_value=0).sort_values(ascending=False)
        all_teams = all_teams.reset_index()
        all_teams.columns = ['team', 'appearances']
        return all_teams
    
    # Get team appearances
    team_appearances = get_team_appearances(filtered_df)
    
    # Show top N teams
    top_n = min(15, len(team_appearances))
    top_teams = team_appearances.head(top_n)
    
    # Create horizontal bar chart
    fig = px.bar(
        top_teams,
        y='team',
        x='appearances',
        orientation='h',
        title=f"Top {top_n} Teams by Match Appearances",
        labels={'appearances': 'Number of Matches', 'team': 'Team'},
        color='appearances',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # Matches by day of week
    st.subheader("Matches by Day of Week")
    
    # Add day of week column
    filtered_df['day_of_week'] = filtered_df['date'].dt.day_name()
    
    # Define the correct order of days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Count matches by day of week
    day_counts = filtered_df['day_of_week'].value_counts().reindex(day_order).reset_index()
    day_counts.columns = ['day_of_week', 'count']
    
    # Create bar chart
    fig = px.bar(
        day_counts,
        x='day_of_week',
        y='count',
        title="Matches by Day of Week",
        labels={'day_of_week': 'Day of Week', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=px.colors.sequential.Greens
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # League distribution by day
    st.subheader("League Distribution by Day")
    
    # Use the analyzer to get the distribution
    league_by_day = analyzer.get_league_distribution_by_day()
    
    if not league_by_day.empty:
        # Sort the day columns in correct order
        day_columns = [day for day in day_order if day in league_by_day.columns]
        league_by_day = league_by_day[day_columns]
        
        # Select top leagues for better visualization
        top_leagues = league_by_day.sum(axis=1).sort_values(ascending=False).head(15).index
        league_by_day_filtered = league_by_day.loc[top_leagues]
        
        fig = px.imshow(
            league_by_day_filtered,
            title="Match Distribution by League and Day of Week (Top 15 Leagues)",
            labels=dict(x="Day of Week", y="League", color="Match Count"),
            color_continuous_scale="Viridis",
            aspect="auto"
        )
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header("Team Analysis")
    
    if selected_team != "All Teams":
        st.subheader(f"Analysis for {selected_team}")
        
        # Count home and away matches
        home_matches = filtered_df[filtered_df['home_team'] == selected_team]
        away_matches = filtered_df[filtered_df['away_team'] == selected_team]
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Home Matches:** {len(home_matches)}")
        with col2:
            st.markdown(f"**Away Matches:** {len(away_matches)}")
        
        # List all matches
        st.subheader("Upcoming Matches")
        team_matches = pd.concat([home_matches, away_matches]).sort_values('date')
        
        for _, match in team_matches.iterrows():
            is_home = match['home_team'] == selected_team
            opponent = match['away_team'] if is_home else match['home_team']
            location = "Home" if is_home else "Away"
            
            st.markdown(f"""
            <div class="match-card">
                <strong>{match['home_team']} vs {match['away_team']}</strong>
                <span style="float:right">{match['date'].strftime('%Y-%m-%d')} • {match['start_time']}</span>
                <p>{match['league']} • {location} match against {opponent}</p>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Select a team from the sidebar to view detailed team analysis.")

# Footer with data source information
st.markdown("---")
st.markdown("⚽ Football Intelligence Dashboard | Data source: SofaScore & FBref | Last updated: " + 
            datetime.now().strftime("%Y-%m-%d %H:%M"))