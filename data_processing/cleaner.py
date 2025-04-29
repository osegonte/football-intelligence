import pandas as pd

class FootballDataCleaner:
    def __init__(self):
        self.standardized_leagues = {
            # Map various league name formats to standard names
            "Premier League": ["EPL", "English Premier League", "Premier League England"],
            "La Liga": ["LaLiga", "LaLiga Santander", "Spanish Primera División"],
            # Add more mappings
        }
    
    def standardize_team_names(self, df):
        # Standardize team names that might appear differently across sources
        team_mapping = {
            "Manchester United FC": "Manchester United",
            "Real Madrid CF": "Real Madrid",
            # Add more mappings
        }
        
        df['home_team'] = df['home_team'].replace(team_mapping)
        df['away_team'] = df['away_team'].replace(team_mapping)
        return df
    
    def standardize_league_names(self, df):
        # Apply league name standardization
        for standard_name, variations in self.standardized_leagues.items():
            df.loc[df['league'].isin(variations), 'league'] = standard_name
        return df
    
    def process_data(self, input_file):
        # Read data
        df = pd.read_csv(input_file)
        
        # Clean and standardize
        df = self.standardize_team_names(df)
        df = self.standardize_league_names(df)
        
        # Convert timestamps
        if 'start_timestamp' in df.columns:
            df['start_datetime'] = pd.to_datetime(df['start_timestamp'], unit='s')
        
        # Additional cleaning steps
        df.dropna(subset=['home_team', 'away_team'], inplace=True)
        
        return df