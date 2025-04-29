import pandas as pd
import numpy as np

class FootballDataAnalyzer:
    def __init__(self, clean_data_path):
        self.df = pd.read_csv(clean_data_path)
    
    def get_matches_by_league(self):
        """Count matches per league"""
        return self.df['league'].value_counts().reset_index()
    
    def get_matches_by_country(self):
        """Count matches per country"""
        return self.df['country'].value_counts().reset_index()
    
    def get_team_appearances(self):
        """Count how many times each team appears in matches"""
        home_teams = self.df['home_team'].value_counts()
        away_teams = self.df['away_team'].value_counts()
        
        # Combine home and away appearances
        all_teams = home_teams.add(away_teams, fill_value=0).sort_values(ascending=False)
        return all_teams.reset_index()
    
    def get_league_distribution_by_day(self):
        """Create a pivot table of match counts by league and day"""
        self.df['date'] = pd.to_datetime(self.df['date'])
        pivot = self.df.pivot_table(
            index='league', 
            columns=self.df['date'].dt.day_name(),
            values='id', 
            aggfunc='count', 
            fill_value=0
        )
        return pivot