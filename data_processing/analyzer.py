import pandas as pd
import numpy as np

class FootballDataAnalyzer:
    """
    Class for analyzing football match data from CSV files.
    """
    def __init__(self, csv_file_path):
        """
        Initialize the analyzer with a CSV file.
        
        Args:
            csv_file_path: Path to the CSV file with match data
        """
        self.df = pd.read_csv(csv_file_path)
        
        # Convert date to datetime if needed
        if 'date' in self.df.columns and self.df['date'].dtype == 'object':
            self.df['date'] = pd.to_datetime(self.df['date'])
    
    def get_matches_by_league(self):
        """Count matches per league"""
        league_counts = self.df['league'].value_counts().reset_index()
        league_counts.columns = ['league', 'count']
        return league_counts
    
    def get_matches_by_country(self):
        """Count matches per country"""
        country_counts = self.df['country'].value_counts().reset_index()
        country_counts.columns = ['country', 'count']
        return country_counts
    
    def get_team_appearances(self):
        """Count how many times each team appears in matches"""
        home_teams = self.df['home_team'].value_counts()
        away_teams = self.df['away_team'].value_counts()
        
        # Combine home and away appearances
        all_teams = home_teams.add(away_teams, fill_value=0).sort_values(ascending=False)
        all_teams = all_teams.reset_index()
        all_teams.columns = ['team', 'appearances']
        return all_teams
    
    def get_team_matches(self, team_name):
        """
        Get all matches for a specific team
        
        Args:
            team_name: Name of the team to filter by
            
        Returns:
            DataFrame with the team's matches
        """
        team_matches = self.df[(self.df['home_team'] == team_name) | 
                               (self.df['away_team'] == team_name)].copy()
        
        # Add is_home column
        team_matches['is_home'] = team_matches['home_team'] == team_name
        
        # Add opponent column
        team_matches['opponent'] = team_matches.apply(
            lambda row: row['away_team'] if row['home_team'] == team_name else row['home_team'], 
            axis=1
        )
        
        return team_matches
    
    def get_league_distribution_by_day(self):
        """Create a pivot table of match counts by league and day"""
        self.df['day_of_week'] = self.df['date'].dt.day_name()
        pivot = self.df.pivot_table(
            index='league', 
            columns='day_of_week',
            values='id', 
            aggfunc='count', 
            fill_value=0
        )
        return pivot
    
    def get_matches_by_date_range(self, start_date, end_date):
        """
        Filter matches by date range
        
        Args:
            start_date: Start date (string or datetime)
            end_date: End date (string or datetime)
            
        Returns:
            DataFrame with matches in the specified date range
        """
        # Convert dates to datetime if they're strings
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
            
        return self.df[(self.df['date'] >= start_date) & (self.df['date'] <= end_date)]
    
    def get_matches_by_day(self):
        """Count matches by day of the week"""
        if 'day_of_week' not in self.df.columns:
            self.df['day_of_week'] = self.df['date'].dt.day_name()
            
        day_counts = self.df['day_of_week'].value_counts().reset_index()
        day_counts.columns = ['day_of_week', 'count']
        
        # Reorder days correctly
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_counts['day_of_week'] = pd.Categorical(
            day_counts['day_of_week'], 
            categories=days_order, 
            ordered=True
        )
        day_counts = day_counts.sort_values('day_of_week')
        
        return day_counts