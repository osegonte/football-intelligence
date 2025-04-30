# dashboard/app.py
# Modified to use the new data loader with database support

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys
import numpy as np

# Add parent directory to path for imports using absolute paths
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Import our new data loader
from dashboard.data_loader import FootballDataLoader
from dashboard.visualizations import (
    create_matches_by_league_chart,
    create_matches_by_country_chart,
    create_match_calendar_heatmap,
    create_team_appearance_chart,
    create_matches_by_day_chart,
    create_league_day_heatmap,
    create_match_timeline
)

# Set page configuration with improved layout
st.set_page_config(
    page_title="Football Intelligence",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply our existing custom CSS (unmodified)
st.markdown("""
<style>
    /* Global typography using SF Pro or system font */
    * {
        font-family: -apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif;
    }
    
    /* (Rest of the CSS styling remains the same) */
</style>
""", unsafe_allow_html=True)

# Title and description using SF Pro styling
st.markdown("""
# ⚽ Football Match Intelligence
""")

st.markdown("""
<p style="color: #6e6e73; font-size: 17px !important; margin-top: -8px; margin-bottom: 24px;">
Advanced football analytics dashboard with match data from major leagues
</p>
""", unsafe_allow_html=True)

# Initialize the data loader
# Try to use database first, with fallback to CSV
try:
    data_loader = FootballDataLoader(use_db=True, csv_fallback=True)
    df = data_loader.load_fixtures()
    source_info = data_loader.get_data_source_info()
    st.sidebar.success(f"Data source: {source_info['source']}")
except Exception as e:
    st.error(f"⚠️ Error loading data: {str(e)}")
    st.info("Please check your database connection or run the scraper to collect match data.")
    st.stop()

# Create sidebar with Apple-inspired styling
with st.sidebar:
    st.markdown("<h3 style='margin-top: 0; font-size: 17px !important; font-weight: 600;'>Filters</h3>", unsafe_allow_html=True)
    st.markdown("<div style='height: 8px'></div>", unsafe_allow_html=True)
    
    # Date range filter with improved styling
    min_date, max_date = data_loader.get_date_range()
    min_date = min_date.date()
    max_date = max_date.date()
    
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
    else:
        start_date, end_date = min_date, max_date
        
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)
    
    # League filter with improved styling
    all_leagues = data_loader.get_leagues()
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
    
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)
    
    # Country filter with improved styling
    all_countries = data_loader.get_countries()
    
    st.markdown("<p style='margin-bottom: 4px; font-weight: 500; font-size: 15px !important;'>Countries</p>", unsafe_allow_html=True)
    selected_countries = st.multiselect(
        label="Select countries",
        options=all_countries,
        default=[],
        label_visibility="collapsed"
    )
    
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)
    
    # Team filter with improved styling
    all_teams = data_loader.get_teams()
    
    st.markdown("<p style='margin-bottom: 4px; font-weight: 500; font-size: 15px !important;'>Team</p>", unsafe_allow_html=True)
    selected_team = st.selectbox(
        label="Filter by team",
        options=["All Teams"] + all_teams,
        index=0,
        label_visibility="collapsed"
    )
    
    # Filter the data based on selections
    filtered_df = data_loader.filter_fixtures(
        start_date=start_date,
        end_date=end_date,
        leagues=selected_leagues if selected_leagues else None,
        countries=selected_countries if selected_countries else None,
        team=selected_team if selected_team != "All Teams" else None
    )
    
    # Add a divider
    st.markdown("<hr style='margin: 24px 0; opacity: 0.2;'>", unsafe_allow_html=True)
    
    # Data source information
    st.markdown("<h3 style='font-size: 15px !important; font-weight: 600;'>Data Info</h3>", unsafe_allow_html=True)
    st.markdown(f"""
    <small style='color: #6e6e73; line-height: 1.4;'>
    <b>Source:</b> {source_info['source']}<br>
    <b>Total Matches:</b> {source_info['total_matches']}<br>
    <b>Date Range:</b> {source_info['date_range']}<br>
    <b>Leagues:</b> {source_info['leagues']}<br>
    <b>Countries:</b> {source_info['countries']}<br>
    <b>Teams:</b> {source_info['teams']}
    </small>
    """, unsafe_allow_html=True)
    
    # Add about section
    st.markdown("<hr style='margin: 24px 0; opacity: 0.2;'>", unsafe_allow_html=True)
    st.markdown("<h3 style='font-size: 15px !important; font-weight: 600;'>About</h3>", unsafe_allow_html=True)
    st.markdown("""
    <small style='color: #6e6e73; line-height: 1.4;'>
    Football Intelligence Dashboard provides analytics and insights for football matches 
    across major leagues. Data is collected from multiple sources including SofaScore and FBref.
    </small>
    """, unsafe_allow_html=True)

# Create Apple-inspired segmented control for tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📆 Schedule", "📈 Analytics", "🔍 Teams"])

with tab1:
    st.markdown("<div class='section'>", unsafe_allow_html=True)
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
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Charts section
    st.markdown("<div class='section'>", unsafe_allow_html=True)
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
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Add calendar heatmap
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.markdown("### Match Calendar")
    fig = create_match_calendar_heatmap(filtered_df)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Add match timeline
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.markdown("### Match Timeline by Hour")
    fig = create_match_timeline(filtered_df)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with tab2:
    st.markdown("## Match Schedule")
    
    # Get matches grouped by date
    matches_by_date = data_loader.get_matches_by_date(filtered_df)
    
    # Display matches for each date
    for date, day_matches in matches_by_date.items():
        # Format date header in Apple style
        st.markdown(f"""
        <h3 style="margin-top: 24px; margin-bottom: 16px; font-weight: 600;">
            {date.strftime('%A, %B %d, %Y')}
        </h3>
        """, unsafe_allow_html=True)
        
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
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.markdown("### Team Appearances")
    team_appearances = data_loader.get_team_appearances(filtered_df=filtered_df)
    fig = create_team_appearance_chart(filtered_df)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    
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
        
        # Get team matches
        team_matches = data_loader.get_matches_for_team(selected_team, filtered_df)
        
        # Count home and away matches
        home_matches = team_matches[team_matches['is_home']]
        away_matches = team_matches[~team_matches['is_home']]
        
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
                <h2>{len(team_matches)}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        # Add spacing
        st.markdown("<div style='height: 24px'></div>", unsafe_allow_html=True)
        
        # List all matches
        st.markdown("### Upcoming Matches")
        
        # Sort matches by date
        team_matches = team_matches.sort_values('date')
        
        for _, match in team_matches.iterrows():
            is_home = match['is_home']
            opponent = match['opponent']
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
                        <div style="font-weight: 600;">{match['date'].strftime('%a, %b %d')}</div>
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
    ⚽ Football Intelligence Dashboard | Data source: {source_info['source']} | Last updated: {datetime.now().strftime("%b %d, %Y")}
</footer>
""", unsafe_allow_html=True)