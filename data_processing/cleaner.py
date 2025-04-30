import pandas as pd
import os

class FootballDataCleaner:
    """
    Class for cleaning and standardizing football match data from CSV files.
    """
    def __init__(self):
        """Initialize the cleaner with standardization mappings"""
        self.standardized_leagues = {
            # Map various league name formats to standard names
            "Premier League": ["EPL", "English Premier League", "Premier League England", "Premier League - England"],
            "La Liga": ["LaLiga", "LaLiga Santander", "Spanish Primera División", "Primera Division"],
            "Bundesliga": ["German Bundesliga", "Bundesliga - Germany", "1. Bundesliga"],
            "Serie A": ["Italian Serie A", "Serie A - Italy", "Serie A TIM"],
            "Ligue 1": ["French Ligue 1", "Ligue 1 - France", "Ligue 1 Uber Eats"],
            "Champions League": ["UEFA Champions League", "UCL", "Champions League (UEFA)"],
            "Europa League": ["UEFA Europa League", "UEL", "Europa League (UEFA)"],
        }
        
        self.standardized_countries = {
            "England": ["UK", "United Kingdom", "Britain", "Great Britain"],
            "United States": ["USA", "US", "U.S.A.", "U.S."],
            "Germany": ["DEU", "GER"],
            "Spain": ["ESP", "Espana"],
            "Italy": ["ITA", "Italia"],
            "France": ["FRA"],
        }
    
    def standardize_team_names(self, df):
        """
        Standardize team names
        
        Args:
            df: DataFrame with match data
            
        Returns:
            DataFrame with standardized team names
        """
        # Standardize team names that might appear differently across sources
        team_mapping = {
            "Manchester United FC": "Manchester United",
            "Manchester City FC": "Manchester City",
            "Liverpool FC": "Liverpool",
            "Chelsea FC": "Chelsea",
            "Arsenal FC": "Arsenal",
            "Real Madrid CF": "Real Madrid",
            "FC Barcelona": "Barcelona",
            "Bayern Munich": "Bayern München",
            "Bayern Munchen": "Bayern München",
            "FC Bayern Munich": "Bayern München",
            "Paris Saint-Germain": "PSG",
            "Paris Saint Germain": "PSG",
            "Paris SG": "PSG",
            "Juventus FC": "Juventus",
            "AC Milan": "Milan",
            "Inter Milan": "Inter",
            "FC Internazionale Milano": "Inter",
        }
        
        df = df.copy()
        df['home_team'] = df['home_team'].replace(team_mapping)
        df['away_team'] = df['away_team'].replace(team_mapping)
        return df
    
    def standardize_league_names(self, df):
        """
        Standardize league names
        
        Args:
            df: DataFrame with match data
            
        Returns:
            DataFrame with standardized league names
        """
        df = df.copy()
        
        # Apply league name standardization
        for standard_name, variations in self.standardized_leagues.items():
            df.loc[df['league'].isin(variations), 'league'] = standard_name
            
        return df
    
    def standardize_country_names(self, df):
        """
        Standardize country names
        
        Args:
            df: DataFrame with match data
            
        Returns:
            DataFrame with standardized country names
        """
        df = df.copy()
        
        # Apply country name standardization
        for standard_name, variations in self.standardized_countries.items():
            df.loc[df['country'].isin(variations), 'country'] = standard_name
            
        return df
    
    def process_data(self, input_file, output_file=None):
        """
        Clean and standardize data from a CSV file
        
        Args:
            input_file: Path to the input CSV file
            output_file: Path to save the processed data (if None, replaces the input file)
            
        Returns:
            Processed DataFrame
        """
        # Read data
        df = pd.read_csv(input_file)
        
        # Clean and standardize
        df = self.standardize_team_names(df)
        df = self.standardize_league_names(df)
        df = self.standardize_country_names(df)
        
        # Ensure date is in datetime format
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            
        # Convert timestamps if present
        if 'start_timestamp' in df.columns and pd.api.types.is_numeric_dtype(df['start_timestamp']):
            df['start_datetime'] = pd.to_datetime(df['start_timestamp'], unit='s')
            
            # Extract just time if it doesn't exist
            if 'start_time' not in df.columns:
                df['start_time'] = df['start_datetime'].dt.strftime('%H:%M')
        
        # Additional cleaning steps
        df.dropna(subset=['home_team', 'away_team'], inplace=True)
        
        # Save processed data if output file specified
        if output_file:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
            df.to_csv(output_file, index=False)
        elif output_file is None and input_file:
            # Replace input file
            df.to_csv(input_file, index=False)
            
        return df
    
    def merge_csv_files(self, input_files, output_file):
        """
        Merge multiple CSV files into one
        
        Args:
            input_files: List of input CSV file paths
            output_file: Path to save the merged data
            
        Returns:
            Merged DataFrame
        """
        # Read and combine all files
        dfs = []
        for file in input_files:
            if os.path.exists(file):
                df = pd.read_csv(file)
                dfs.append(df)
        
        if not dfs:
            raise ValueError("No valid input files found")
            
        # Concatenate all dataframes
        merged_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates based on all columns except potential ID columns
        columns_for_dedup = [col for col in merged_df.columns if col not in ['id', 'index']]
        merged_df = merged_df.drop_duplicates(subset=columns_for_dedup)
        
        # Clean and standardize the merged data
        merged_df = self.standardize_team_names(merged_df)
        merged_df = self.standardize_league_names(merged_df)
        merged_df = self.standardize_country_names(merged_df)
        
        # Save merged data
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        merged_df.to_csv(output_file, index=False)
        
        return merged_df