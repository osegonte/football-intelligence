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
from visualizations import (
    create_matches_by_league_chart,
    create_matches_by_country_chart,
    create_match_calendar_heatmap,
    create_team_appearance_chart,
    create_matches_by_day_chart,
    create_league_day_heatmap,
    create_match_timeline
)

# Add parent directory to path for imports using absolute paths
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from data_processing.analyzer import FootballDataAnalyzer

# Set page configuration with improved layout
st.set_page_config(
    page_title="Football Intelligence",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply Apple-inspired custom CSS
st.markdown("""
<style>
    /* Global typography using SF Pro or system font */
    * {
        font-family: -apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif;
    }
    
    /* Typography scale */
    h1 {
        font-size: 34px !important;
        font-weight: 800 !important;
        letter-spacing: -0.03em;
        color: #1d1d1f;
    }
    h2 {
        font-size: 28px !important;
        font-weight: 700 !important;
        letter-spacing: -0.02em;
        color: #1d1d1f;
        margin-top: 1.5rem !important;
    }
    h3 {
        font-size: 20px !important;
        font-weight: 600 !important;
        color: #1d1d1f;
    }
    p, div, li {
        font-size: 17px !important;
        color: #424245;
    }
    small {
        font-size: 13px !important;
        color: #6e6e73;
    }
    
    /* Dark mode support */
    @media (prefers-color-scheme: dark) {
        h1, h2, h3 {
            color: #f5f5f7;
        }
        p, div, li {
            color: #a1a1a6;
        }
        small {
            color: #86868b;
        }
    }
    
    /* Layout & Spacing - 8-point grid */
    .main > div {
        padding: 0 24px;
        max-width: 1200px;
        margin: 0 auto;
    }
    
    /* Card styling */
    .card {
        background-color: white;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        margin-bottom: 16px;
        border: 1px solid rgba(0,0,0,0.1);
        transition: all 0.2s ease;
    }
    .card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        transform: translateY(-2px);
    }
    
    /* Dark mode for cards */
    @media (prefers-color-scheme: dark) {
        .card {
            background-color: #1c1c1e;
            border: 1px solid rgba(255,255,255,0.1);
        }
    }
    
    /* Tabs styling */
    .stTabs {
        background-color: transparent !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background-color: transparent;
        border-bottom: 1px solid rgba(0,0,0,0.1);
        padding-bottom: 0;
    }
    .stTabs [data-baseweb="tab"] {
        height: 44px;
        background-color: transparent;
        border: none;
        color: #424245;
        font-size: 15px;
        font-weight: 500;
        transition: all 0.2s ease;
        border-radius: 8px 8px 0 0;
        padding: 0 16px;
    }
    .stTabs [aria-selected="true"] {
        background-color: transparent;
        color: #007aff;
        border-bottom: 2px solid #007aff;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background-color: rgba(0,0,0,0.05);
        color: #007aff;
    }
    
    /* Metric cards */
    .metric-container {
        background-color: white;
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        border: 1px solid rgba(0,0,0,0.08);
        height: 100%;
        transition: all 0.2s ease;
    }
    .metric-container:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.08);
    }
    .metric-container h3 {
        margin-bottom: 4px;
        font-size: 16px !important;
        color: #6e6e73;
    }
    .metric-container h2 {
        margin-top: 0 !important;
        font-size: 32px !important;
        color: #007aff;
    }
    
    /* Match cards */
    .match-card {
        background-color: white;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        border: 1px solid rgba(0,0,0,0.08);
        transition: all 0.2s ease;
    }
    .match-card:hover {
        box-shadow: 0 2px 6px rgba(0,0,0,0.08);
        transform: translateY(-1px);
    }
    .match-card strong {
        font-weight: 600;
    }
    
    /* League headers */
    .league-header {
        background-color: #f5f5f7;
        padding: 8px 16px;
        border-radius: 8px;
        margin-bottom: 8px;
        font-weight: 600;
        font-size: 15px !important;
        color: #1d1d1f;
    }
    
    /* Filter controls */
    .sidebar .stDateInput, .sidebar .stMultiselect, .sidebar .stSelectbox {
        background-color: #f5f5f7;
        border-radius: 8px;
        padding: 8px;
        margin-bottom: 16px;
    }
    
    /* Button styling */
    .stButton > button {
        border-radius: 8px;
        background-color: #007aff;
        color: white;
        font-weight: 500;
        transition: all 0.2s ease;
        border: none;
        padding: 8px 16px;
    }
    .stButton > button:hover {
        background-color: #0063cc;
        color: white;
    }
    
    /* Footer styling */
    footer {
        border-top: 1px solid rgba(0,0,0,0.1);
        padding-top: 16px;
        margin-top: 48px;
        color: #86868b;
        font-size: 13px !important;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: rgba(0,0,0,0.05);
        border-radius: 8px;
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(0,0,0,0.2);
        border-radius: 8px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(0,0,0,0.3);
    }
    
    /* Container widths */
    .container {
        padding: 0;
        max-width: 100%;
    }
    
    /* Sidebar width */
    [data-testid="stSidebar"] {
        min-width: 240px !important;
        max-width: 240px !important;
    }
    
    /* Sidebar padding */
    [data-testid="stSidebar"] > div {
        padding-top: 2rem;
    }
    
    /* Filter pills */
    .filter-pill {
        display: inline-block;
        background-color: #f5f5f7;
        border-radius: 16px;
        padding: 6px 12px;
        margin-right: 8px;
        margin-bottom: 8px;
        font-size: 14px !important;
        cursor: pointer;
        transition: all 0.2s ease;
    }
    .filter-pill.active {
        background-color: #007aff;
        color: white;
    }
    .filter-pill:hover {
        background-color: #e5e5ea;
    }
    .filter-pill.active:hover {
        background-color: #0063cc;
    }
</style>
""", unsafe_allow_html=True)

# Title and description - using SF Pro styling
st.markdown("""
# ⚽ Football Intelligence Dashboard
""")

st.markdown("""
<p style="color: #6e6e73; font-size: 17px !important; margin-top: -8px; margin-bottom: 24px;">
Analyzing football matches across major leagues with data from SofaScore and FBref
</p>
""", unsafe_allow_html=True)

# Check if data file exists
data_file = os.path.join(parent_dir, "data", "all_matches_latest.csv")
if not os.path.exists(data_file):
    data_file = os.path.join(parent_dir, "sofascore_data", "all_matches_latest.csv")

if not os.path.exists(data_file):
    st.error("⚠️ Data file not found. Please run the scraper first to collect match data.")
    st.info("You can run the scraper with: `python main.py --days 7 --stats`")
    st.stop()

# Load data with caching
@st.cache_data
def load_data():
    df = pd.read_csv(data_file)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    return df

df = load_data()

# Create analyzer
analyzer = FootballDataAnalyzer(data_file)

# Create sidebar with Apple-inspired styling
with st.sidebar:
    st.markdown("## Filters")
    st.markdown("<div style='height: 8px'></div>", unsafe_allow_html=True)
    
    # Date range filter with improved styling
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    
    # Ensure the default end date doesn't exceed the maximum date in the dataset
    default_end_date = min(min_date + timedelta(days=7), max_date)
    
    st.markdown("<p style='margin-bottom: 4px; font-weight: 500; font-size: 15px !important;'>Date Range</p>", unsafe_allow_html=True)
    date_range = st.date_input(
        label="Select dates",
        value=(min_date, default_end_date),
        min_value=min_date,
        max_value=max_date,
        label_visibility="collapsed"
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = df[(df['date'].dt.date >= start_date) & 
                        (df['date'].dt.date <= end_date)]
    else:
        filtered_df = df
        
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)
    
    # League filter with improved styling
    all_leagues = sorted(df['league'].unique())
    default_leagues = ["Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"]
    # Ensure default leagues actually exist in the data
    default_leagues = [league for league in default_leagues if league in all_leagues]
    
    st.markdown("<p style='margin-bottom: 4px; font-weight: 500; font-size: 15px !important;'>Leagues</p>", unsafe_allow_html=True)
    selected_leagues = st.multiselect(
        label="Select leagues",
        options=all_leagues,
        default=default_leagues if default_leagues else all_leagues[:5],
        label_visibility="collapsed"
    )
    
    if selected_leagues:
        filtered_df = filtered_df[filtered_df['league'].isin(selected_leagues)]
    
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)
    
    # Country filter with improved styling
    all_countries = sorted(filtered_df['country'].unique())
    
    st.markdown("<p style='margin-bottom: 4px; font-weight: 500; font-size: 15px !important;'>Countries</p>", unsafe_allow_html=True)
    selected_countries = st.multiselect(
        label="Select countries",
        options=all_countries,
        default=[],
        label_visibility="collapsed"
    )
    
    if selected_countries:
        filtered_df = filtered_df[filtered_df['country'].isin(selected_countries)]
    
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)
    
    # Team filter with improved styling
    all_teams = set()
    for team in filtered_df['home_team'].unique():
        all_teams.add(team)
    for team in filtered_df['away_team'].unique():
        all_teams.add(team)
    all_teams = sorted(list(all_teams))
    
    st.markdown("<p style='margin-bottom: 4px; font-weight: 500; font-size: 15px !important;'>Team</p>", unsafe_allow_html=True)
    selected_team = st.selectbox(
        label="Filter by team",
        options=["All Teams"] + all_teams,
        index=0,
        label_visibility="collapsed"
    )
    
    if selected_team != "All Teams":
        filtered_df = filtered_df[(filtered_df['home_team'] == selected_team) | 
                                (filtered_df['away_team'] == selected_team)]
    
    # Add a divider
    st.markdown("<hr style='margin: 24px 0; opacity: 0.3;'>", unsafe_allow_html=True)
    
    # Add about section
    st.markdown("### About")
    st.markdown("""
    <small style='color: #6e6e73;'>
    Football Intelligence Dashboard provides analytics and insights for football matches 
    across major leagues. Data is collected from multiple sources including SofaScore and FBref.
    </small>
    """, unsafe_allow_html=True)

# Create Apple-inspired tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📆 Schedule", "📈 Analytics", "🔍 Teams"])

with tab1:
    st.markdown("## Match Overview")
    
    # Match count metrics in Apple-inspired card layout
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-container">
            <h3>Total Matches</h3>
            <h2>{len(filtered_df)}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-container">
            <h3>Leagues</h3>
            <h2>{filtered_df['league'].nunique()}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-container">
            <h3>Countries</h3>
            <h2>{filtered_df['country'].nunique()}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        days_count = filtered_df['date'].dt.date.nunique()
        st.markdown(f"""
        <div class="metric-container">
            <h3>Days</h3>
            <h2>{days_count}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    # Add some space
    st.markdown("<div style='height: 24px'></div>", unsafe_allow_html=True)
    
    # Charts section
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Matches by League")
        
        # Use the enhanced chart function
        fig = create_matches_by_league_chart(filtered_df)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### Matches by Country")
        
        # Use the enhanced chart function
        fig = create_matches_by_country_chart(filtered_df)
        st.plotly_chart(fig, use_container_width=True)
    
    # Add calendar heatmap
    st.markdown("### Match Calendar")
    fig = create_match_calendar_heatmap(filtered_df)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    
    # Add match timeline
    st.markdown("### Match Timeline by Hour")
    fig = create_match_timeline(filtered_df)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.markdown("## Match Schedule")
    
    # Group by date and display matches
    filtered_df = filtered_df.sort_values(['date', 'start_time'])
    dates = filtered_df['date'].dt.date.unique()
    
    for date in dates:
        # Format date header in Apple style
        st.markdown(f"""
        <h3 style="margin-top: 24px; margin-bottom: 16px; font-weight: 600;">
            {date.strftime('%A, %B %d, %Y')}
        </h3>
        """, unsafe_allow_html=True)
        
        day_matches = filtered_df[filtered_df['date'].dt.date == date]
        
        # Group by league for better organization
        leagues = sorted(day_matches['league'].unique())
        
        for league in leagues:
            league_matches = day_matches[day_matches['league'] == league]
            
            with st.expander(f"{league} ({len(league_matches)} matches)", expanded=True):
                for _, match in league_matches.iterrows():
                    # Status indicator color
                    status_color = "#6e6e73"  # Default gray
                    if match['status'] == 'Ended':
                        status_color = "#8e8e93"  # Darker gray for ended
                    elif match['status'] == '1st half' or match['status'] == '2nd half':
                        status_color = "#34c759"  # Green for live
                    elif match['status'] == 'Not started':
                        status_color = "#007aff"  # Blue for upcoming
                        
                    st.markdown(f"""
                    <div class="match-card">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <strong>{match['home_team']}</strong> vs <strong>{match['away_team']}</strong>
                                <div style="font-size: 13px !important; color: #86868b; margin-top: 4px;">
                                    {match['country']} • {match['league']}
                                </div>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-weight: 600;">{match['start_time']}</div>
                                <div style="font-size: 13px !important; color: {status_color}; margin-top: 4px;">
                                    {match['status']}
                                </div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

with tab3:
    st.markdown("## Advanced Analytics")
    
    # Team appearances chart
    st.markdown("### Team Appearances")
    fig = create_team_appearance_chart(filtered_df)
    st.plotly_chart(fig, use_container_width=True)
    
    # Add some space
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Matches by day of week
        st.markdown("### Matches by Day of Week")
        fig = create_matches_by_day_chart(filtered_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # League distribution by day
        st.markdown("### League Distribution by Day")
        fig = create_league_day_heatmap(filtered_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.markdown("## Team Analysis")
    
    if selected_team != "All Teams":
        st.markdown(f"### Analysis for {selected_team}")
        
        # Count home and away matches
        home_matches = filtered_df[filtered_df['home_team'] == selected_team]
        away_matches = filtered_df[filtered_df['away_team'] == selected_team]
        
        # Team metrics in Apple-style cards
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="metric-container">
                <h3>Home Matches</h3>
                <h2>{len(home_matches)}</h2>
            </div>
            """, unsafe_allow_html=True)
            
        with col2:
            st.markdown(f"""
            <div class="metric-container">
                <h3>Away Matches</h3>
                <h2>{len(away_matches)}</h2>
            </div>
            """, unsafe_allow_html=True)
            
        with col3:
            st.markdown(f"""
            <div class="metric-container">
                <h3>Total Matches</h3>
                <h2>{len(home_matches) + len(away_matches)}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        # Add spacing
        st.markdown("<div style='height: 24px'></div>", unsafe_allow_html=True)
        
        # List all matches
        st.markdown("### Upcoming Matches")
        team_matches = pd.concat([home_matches, away_matches]).sort_values('date')
        
        for _, match in team_matches.iterrows():
            is_home = match['home_team'] == selected_team
            opponent = match['away_team'] if is_home else match['home_team']
            location = "Home" if is_home else "Away"
            
            # Match card styling with Apple-inspired design
            location_color = "#34c759" if is_home else "#ff9500"  # Green for home, orange for away
            
            st.markdown(f"""
            <div class="match-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong>{match['home_team']} vs {match['away_team']}</strong>
                        <div style="margin-top: 4px; color: #86868b; font-size: 13px !important;">
                            {match['league']} • <span style="color: {location_color};">{location}</span> match against {opponent}
                        </div>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-weight: 600;">{match['date'].strftime('%Y-%m-%d')}</div>
                        <div style="color: #86868b; font-size: 13px !important; margin-top: 4px;">
                            {match['start_time']}
                        </div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Select a team from the sidebar to view detailed team analysis.")
        
        # Show top teams preview
        st.markdown("### Top Teams Overview")
        fig = create_team_appearance_chart(filtered_df, top_n=10)
        st.plotly_chart(fig, use_container_width=True)

# Footer with data source information in Apple-style
st.markdown("---")
st.markdown(f"""
<footer>
    ⚽ Football Intelligence Dashboard | Data source: SofaScore & FBref | Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M")}
</footer>
""", unsafe_allow_html=True)