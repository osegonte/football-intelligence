# dashboard/data_loader.py
"""
Simplified data loader module for the Football Intelligence dashboard.
Uses CSV files instead of a database.
"""
import os
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st

class FootballDataLoader:
    """
    Data loader for football match data using CSV storage.
    """
    
    def __init__(self):
        """Initialize the data loader."""
        # Cached data
        self._fixtures = None
        self._leagues = None
        self._countries = None
        self._teams = None
        self.data_source = "CSV"
    
    def _get_csv_path(self):
        """Get path to the fixtures CSV file"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        
        # Try different potential locations
        candidate_paths = [
            os.path.join(parent_dir, "data", "all_matches_latest.csv"),
            os.path.join(parent_dir, "sofascore_data", "all_matches_latest.csv")
        ]
        
        for path in candidate_paths:
            if os.path.exists(path):
                return path
        
        return None
    
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def load_fixtures(self, _self=None, force_reload=False):
        """
        Load fixtures data from CSV
        
        Args:
            _self: Leading underscore to tell Streamlit not to hash this parameter
            force_reload: Force reload from source (ignore cache)
            
        Returns:
            DataFrame with fixtures data
        """
        # Note: _self parameter is ignored, it's just to avoid hashing issues
        if self._fixtures is not None and not force_reload:
            return self._fixtures
            
        csv_path = self._get_csv_path()
        if not csv_path:
            raise FileNotFoundError("No fixture data CSV file found. Please run the scraper first.")
        
        df = pd.read_csv(csv_path)
        
        # Convert date to datetime if it's a string
        if 'date' in df.columns and df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'])
            
        self.data_source = f"CSV: {os.path.basename(csv_path)}"
        self._fixtures = df
        return df
    
    @st.cache_data(ttl=3600)
    def get_date_range(self, _self=None):
        """Get the available date range in the data"""
        # Note: _self parameter is ignored, it's just to avoid hashing issues
        df = self.load_fixtures()
        min_date = df['date'].min()
        max_date = df['date'].max()
        return min_date, max_date
    
    @st.cache_data(ttl=3600)
    def get_leagues(self, _self=None):
        """Get unique leagues in the data"""
        # Note: _self parameter is ignored, it's just to avoid hashing issues
        df = self.load_fixtures()
        return sorted(df['league'].unique())
    
    @st.cache_data(ttl=3600)
    def get_countries(self, _self=None):
        """Get unique countries in the data"""
        # Note: _self parameter is ignored, it's just to avoid hashing issues
        df = self.load_fixtures()
        return sorted(df['country'].unique())
    
    @st.cache_data(ttl=3600)
    def get_teams(self, _self=None):
        """Get unique teams in the data"""
        # Note: _self parameter is ignored, it's just to avoid hashing issues
        df = self.load_fixtures()
        teams = set()
        for team in df['home_team'].unique():
            teams.add(team)
        for team in df['away_team'].unique():
            teams.add(team)
        return sorted(list(teams))
    
    def filter_fixtures(self, start_date=None, end_date=None, leagues=None, countries=None, team=None):
        """
        Filter fixtures based on criteria
        
        Args:
            start_date: Start date for filtering
            end_date: End date for filtering
            leagues: List of leagues to include
            countries: List of countries to include
            team: Team to filter by
            
        Returns:
            DataFrame with filtered fixtures
        """
        df = self.load_fixtures()
        
        # Filter by date
        if start_date:
            df = df[df['date'].dt.date >= start_date]
        if end_date:
            df = df[df['date'].dt.date <= end_date]
            
        # Filter by leagues
        if leagues and len(leagues) > 0:
            df = df[df['league'].isin(leagues)]
            
        # Filter by countries
        if countries and len(countries) > 0:
            df = df[df['country'].isin(countries)]
            
        # Filter by team
        if team and team != "All Teams":
            df = df[(df['home_team'] == team) | (df['away_team'] == team)]
            
        return df
    
    def get_matches_by_date(self, filtered_df=None):
        """
        Group matches by date
        
        Args:
            filtered_df: Filtered DataFrame (if None, uses all fixtures)
            
        Returns:
            Dictionary with dates as keys and match DataFrames as values
        """
        if filtered_df is None:
            filtered_df = self.load_fixtures()
            
        # Ensure date is datetime
        if not pd.api.types.is_datetime64_dtype(filtered_df['date']):
            filtered_df['date'] = pd.to_datetime(filtered_df['date'])
            
        # Sort by date and time
        if 'start_time' in filtered_df.columns:
            filtered_df = filtered_df.sort_values(['date', 'start_time'])
        else:
            filtered_df = filtered_df.sort_values('date')
            
        # Group by date
        dates = filtered_df['date'].dt.date.unique()
        result = {}
        
        for date in dates:
            result[date] = filtered_df[filtered_df['date'].dt.date == date]
            
        return result
    
    def get_team_appearances(self, top_n=None, filtered_df=None):
        """
        Calculate team appearances
        
        Args:
            top_n: Limit to top N teams
            filtered_df: Filtered DataFrame (if None, uses all fixtures)
            
        Returns:
            DataFrame with team appearance counts
        """
        if filtered_df is None:
            filtered_df = self.load_fixtures()
            
        # Count home and away appearances
        home_teams = filtered_df['home_team'].value_counts()
        away_teams = filtered_df['away_team'].value_counts()
        
        # Combine counts
        all_teams = home_teams.add(away_teams, fill_value=0).sort_values(ascending=False)
        all_teams = all_teams.reset_index()
        all_teams.columns = ['team', 'appearances']
        
        # Take top N teams if specified
        if top_n:
            all_teams = all_teams.head(top_n)
            
        return all_teams
    
    def get_matches_for_team(self, team_name, filtered_df=None):
        """
        Get matches for a specific team
        
        Args:
            team_name: Name of the team
            filtered_df: Filtered DataFrame (if None, uses all fixtures)
            
        Returns:
            DataFrame with team's matches
        """
        if filtered_df is None:
            filtered_df = self.load_fixtures()
            
        # Filter matches where the team plays
        team_matches = filtered_df[(filtered_df['home_team'] == team_name) | 
                                   (filtered_df['away_team'] == team_name)].copy()
        
        # Add is_home column
        team_matches['is_home'] = team_matches['home_team'] == team_name
        
        # Add opponent column
        team_matches['opponent'] = team_matches.apply(
            lambda row: row['away_team'] if row['home_team'] == team_name else row['home_team'], 
            axis=1
        )
        
        return team_matches
        
    def get_data_source_info(self):
        """Get information about the data source"""
        df = self.load_fixtures()
        
        return {
            "source": self.data_source,
            "total_matches": len(df),
            "date_range": f"{df['date'].min().date()} to {df['date'].max().date()}",
            "leagues": df['league'].nunique(),
            "countries": df['country'].nunique(),
            "teams": len(self.get_teams())
        }