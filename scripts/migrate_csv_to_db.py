# scripts/migrate_csv_to_db.py
import os
import sys
import pandas as pd
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_session, close_session
from database.repositories.base_repository import BaseRepository
from database.schema import Country, League, Team, Fixture

def import_from_csv(csv_file_path, session):
    """Import matches from CSV to database"""
    print(f"Reading {csv_file_path}...")
    df = pd.read_csv(csv_file_path)
    
    # Create repository
    repo = BaseRepository(session)
    
    # Process countries
    print("Processing countries...")
    unique_countries = df['country'].unique()
    country_map = {}
    
    for country_name in unique_countries:
        country = session.query(Country).filter(Country.name == country_name).first()
        if not country:
            country = Country(name=country_name)
            repo.add(country)
            repo.commit()
        country_map[country_name] = country.id
    
    # Process leagues
    print("Processing leagues...")
    unique_leagues = df[['league', 'country']].drop_duplicates().values.tolist()
    league_map = {}
    
    for league_name, country_name in unique_leagues:
        # Create key for league map
        league_key = f"{league_name}_{country_name}"
        
        # Check if league exists
        league = session.query(League).join(Country).filter(
            League.name == league_name,
            Country.name == country_name
        ).first()
        
        if not league:
            league = League(
                name=league_name,
                country_id=country_map[country_name]
            )
            repo.add(league)
            repo.commit()
        
        league_map[league_key] = league.id
    
    # Process teams
    print("Processing teams...")
    all_teams = set()
    for _, row in df.iterrows():
        all_teams.add(row['home_team'])
        all_teams.add(row['away_team'])
    
    team_map = {}
    for team_name in all_teams:
        team = session.query(Team).filter(Team.name == team_name).first()
        if not team:
            # Assume team belongs to the country of its most common league
            team_matches = df[(df['home_team'] == team_name) | (df['away_team'] == team_name)]
            most_common_country = team_matches['country'].mode()[0]
            
            team = Team(
                name=team_name,
                country_id=country_map[most_common_country]
            )
            repo.add(team)
            repo.commit()
        team_map[team_name] = team.id
    
    # Process fixtures
    print("Processing fixtures...")
    fixtures_added = 0
    for _, row in df.iterrows():
        # Convert date string to datetime
        if 'date' in row and pd.notna(row['date']):
            match_date = pd.to_datetime(row['date']).to_pydatetime()
        else:
            # Skip if no date
            continue
        
        # Get or create fixture
        external_id = str(row.get('id', ''))
        if external_id:
            fixture = session.query(Fixture).filter(Fixture.external_id == external_id).first()
            if fixture:
                # Skip existing fixtures
                continue
        
        league_key = f"{row['league']}_{row['country']}"
        
        fixture = Fixture(
            external_id=external_id,
            home_team_id=team_map[row['home_team']],
            away_team_id=team_map[row['away_team']],
            league_id=league_map[league_key],
            date=match_date,
            start_time=row.get('start_time', None),
            venue=row.get('venue', None),
            status=row.get('status', None),
            round=row.get('round', None),
            source=row.get('source', None),
            scrape_date=datetime.now()
        )
        repo.add(fixture)
        fixtures_added += 1
        
        # Commit in batches to prevent memory issues
        if fixtures_added % 100 == 0:
            repo.commit()
            print(f"Imported {fixtures_added} fixtures...")
    
    # Final commit
    repo.commit()
    print(f"Imported {fixtures_added} fixtures from {csv_file_path}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python migrate_csv_to_db.py <csv_file_or_directory>")
        return 1
    
    path = sys.argv[1]
    session = get_session()
    
    try:
        if os.path.isfile(path) and path.endswith('.csv'):
            # Import single file
            import_from_csv(path, session)
        elif os.path.isdir(path):
            # Import all CSV files in directory
            csv_files = []
            for root, _, files in os.walk(path):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
            
            if not csv_files:
                print(f"No CSV files found in {path}")
                return 1
                
            for csv_file in csv_files:
                import_from_csv(csv_file, session)
        else:
            print(f"Error: {path} is not a valid CSV file or directory")
            return 1
        
        print("Migration completed successfully")
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}")
        session.rollback()
        return 1
        
    finally:
        close_session(session)

if __name__ == "__main__":
    sys.exit(main())