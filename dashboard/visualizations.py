import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def create_matches_by_league_chart(df):
    """Create bar chart of matches by league"""
    league_counts = df['league'].value_counts().reset_index()
    league_counts.columns = ['league', 'count']
    
    fig = px.bar(
        league_counts.head(10),
        x='league',
        y='count',
        title="Top 10 Leagues by Match Count",
        labels={'league': 'League', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=px.colors.sequential.Blues
    )
    return fig

def create_match_calendar_heatmap(df):
    """Create a calendar heatmap of matches by date and league"""
    if 'date' not in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        
    # Create date and day of week columns
    df['day'] = df['date'].dt.day_name()
    df['week'] = df['date'].dt.isocalendar().week
    
    # Create pivot table
    pivot = df.pivot_table(
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
    return fig

def create_team_appearance_chart(df, top_n=20):
    """Create horizontal bar chart of team appearances"""
    # Count home and away appearances
    home_teams = df['home_team'].value_counts()
    away_teams = df['away_team'].value_counts()
    
    # Combine counts
    all_teams = home_teams.add(away_teams, fill_value=0).sort_values(ascending=False)
    all_teams = all_teams.head(top_n).reset_index()
    all_teams.columns = ['team', 'appearances']
    
    fig = px.bar(
        all_teams,
        y='team',
        x='appearances',
        orientation='h',
        title=f"Top {top_n} Teams by Match Appearances",
        labels={'appearances': 'Number of Matches', 'team': 'Team'},
        color='appearances',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    return fig