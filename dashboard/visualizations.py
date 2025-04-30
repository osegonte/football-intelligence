import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np

def create_matches_by_league_chart(df, top_n=10):
    """
    Create an Apple-style bar chart of matches by league
    
    Args:
        df: DataFrame with match data
        top_n: Number of top leagues to display
        
    Returns:
        Plotly figure
    """
    # Get match counts
    league_counts = df['league'].value_counts().reset_index()
    league_counts.columns = ['league', 'count']
    
    # Take top N leagues
    top_n = min(top_n, len(league_counts))
    top_leagues = league_counts.head(top_n)
    
    # Create bar chart with Apple-style colors and design
    fig = px.bar(
        top_leagues,
        x='league',
        y='count',
        title=None,
        labels={'league': '', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=['#76d0ff', '#0071e3'],
        template="plotly_white"
    )
    
    # Add data labels on top of bars
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Matches: %{y}<extra></extra>',
        marker_line_width=0
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_tickangle=-45,
        xaxis_title=None,
        yaxis_title=None,
        coloraxis_showscale=False,
        margin=dict(t=0, b=80, l=20, r=20),
        hoverlabel=dict(bgcolor="white", font_size=14),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'
        ),
        yaxis=dict(
            gridcolor='rgba(0,0,0,0.05)'
        )
    )
    
    return fig

def create_matches_by_country_chart(df, top_n=10):
    """
    Create an Apple-style bar chart of matches by country
    
    Args:
        df: DataFrame with match data
        top_n: Number of top countries to display
        
    Returns:
        Plotly figure
    """
    # Get match counts
    country_counts = df['country'].value_counts().reset_index()
    country_counts.columns = ['country', 'count']
    
    # Take top N countries
    top_n = min(top_n, len(country_counts))
    top_countries = country_counts.head(top_n)
    
    # Create bar chart with Apple-style colors and design
    fig = px.bar(
        top_countries,
        x='country',
        y='count',
        title=None,
        labels={'country': '', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=['#76d0ff', '#0071e3'],
        template="plotly_white"
    )
    
    # Add data labels on top of bars
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Matches: %{y}<extra></extra>',
        marker_line_width=0
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_tickangle=-45,
        xaxis_title=None,
        yaxis_title=None,
        coloraxis_showscale=False,
        margin=dict(t=0, b=80, l=20, r=20),
        hoverlabel=dict(bgcolor="white", font_size=14),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'
        ),
        yaxis=dict(
            gridcolor='rgba(0,0,0,0.05)'
        )
    )
    
    return fig

def create_match_calendar_heatmap(df):
    """
    Create an Apple-style calendar heatmap of matches by date and day of week
    
    Args:
        df: DataFrame with match data
        
    Returns:
        Plotly figure
    """
    # Ensure date is datetime
    if 'date' not in df.columns:
        return None
    
    if not isinstance(df['date'].iloc[0], pd.Timestamp):
        df['date'] = pd.to_datetime(df['date'])
    
    # Extract day of week
    df['day_of_week'] = df['date'].dt.day_name()
    
    # Extract week number (for y-axis)
    df['week'] = df['date'].dt.isocalendar().week
    
    # Create day and week count
    pivot = pd.pivot_table(
        df,
        index='week',
        columns='day_of_week',
        values='id',
        aggfunc='count',
        fill_value=0
    )
    
    # Ensure days are in correct order
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot = pivot.reindex(columns=[day for day in days_order if day in pivot.columns])
    
    # Create heatmap with Apple-style colors
    fig = px.imshow(
        pivot,
        title=None,
        labels=dict(x="", y="", color=""),
        color_continuous_scale=['#f5f5f7', '#0071e3'],
        template="plotly_white",
        aspect="auto",
        text_auto=True
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        coloraxis_colorbar=dict(
            title=None,
            tickfont=dict(size=12)
        ),
        margin=dict(t=0, b=20, l=20, r=20),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        height=300
    )
    
    # Update y-axis to show "Week" labels
    fig.update_yaxes(
        tickvals=list(pivot.index),
        ticktext=[f"Week {w}" for w in pivot.index],
        tickfont=dict(size=12)
    )
    
    # Update text style
    fig.update_traces(
        texttemplate="%{z}",
        textfont=dict(
            family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif",
            size=12,
            color="black"
        )
    )
    
    return fig

def create_team_appearance_chart(df, top_n=15):
    """
    Create an Apple-style horizontal bar chart showing teams with most appearances
    
    Args:
        df: DataFrame with match data
        top_n: Number of top teams to display
        
    Returns:
        Plotly figure
    """
    # Count home and away appearances
    home_teams = df['home_team'].value_counts()
    away_teams = df['away_team'].value_counts()
    
    # Combine counts
    all_teams = home_teams.add(away_teams, fill_value=0).sort_values(ascending=False)
    all_teams = all_teams.reset_index()
    all_teams.columns = ['team', 'appearances']
    
    # Take top N teams
    top_n = min(top_n, len(all_teams))
    top_teams = all_teams.head(top_n)
    
    # Create horizontal bar chart with Apple-style colors
    fig = px.bar(
        top_teams,
        y='team',
        x='appearances',
        orientation='h',
        title=None,
        labels={'appearances': 'Number of Matches', 'team': ''},
        color='appearances',
        color_continuous_scale=['#76d0ff', '#0071e3'],
        template="plotly_white"
    )
    
    # Update the order of teams
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    # Add data labels
    fig.update_traces(
        texttemplate='%{x}',
        textposition='outside',
        hovertemplate='<b>%{y}</b><br>Matches: %{x}<extra></extra>',
        marker_line_width=0
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=None,
        coloraxis_showscale=False,
        margin=dict(t=0, b=20, l=120, r=80),
        hoverlabel=dict(bgcolor="white", font_size=14),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'
        ),
        yaxis=dict(
            tickfont=dict(size=12)
        )
    )
    
    return fig

def create_matches_by_day_chart(df):
    """
    Create an Apple-style bar chart showing match distribution by day of week
    
    Args:
        df: DataFrame with match data
        
    Returns:
        Plotly figure
    """
    # Ensure date is datetime
    if 'date' not in df.columns:
        return None
    
    if not isinstance(df['date'].iloc[0], pd.Timestamp):
        df['date'] = pd.to_datetime(df['date'])
    
    # Add day of week column
    df['day_of_week'] = df['date'].dt.day_name()
    
    # Define the correct order of days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Count matches by day of week
    day_counts = df['day_of_week'].value_counts().reindex(day_order).reset_index()
    day_counts.columns = ['day_of_week', 'count']
    
    # Create bar chart with Apple-style colors
    fig = px.bar(
        day_counts,
        x='day_of_week',
        y='count',
        title=None,
        labels={'day_of_week': '', 'count': 'Number of Matches'},
        color='count',
        color_continuous_scale=['#76d0ff', '#0071e3'],
        template="plotly_white"
    )
    
    # Add data labels on top of bars
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Matches: %{y}<extra></extra>',
        marker_line_width=0
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=None,
        coloraxis_showscale=False,
        margin=dict(t=0, b=20, l=20, r=20),
        hoverlabel=dict(bgcolor="white", font_size=14),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'
        ),
        yaxis=dict(
            gridcolor='rgba(0,0,0,0.05)'
        )
    )
    
    return fig

def create_league_day_heatmap(df, top_n=15):
    """
    Create an Apple-style heatmap showing league distribution by day of week
    
    Args:
        df: DataFrame with match data
        top_n: Number of top leagues to include
        
    Returns:
        Plotly figure
    """
    # Ensure date is datetime
    if 'date' not in df.columns or 'league' not in df.columns:
        return None
    
    if not isinstance(df['date'].iloc[0], pd.Timestamp):
        df['date'] = pd.to_datetime(df['date'])
    
    # Add day of week column
    df['day_of_week'] = df['date'].dt.day_name()
    
    # Define the correct order of days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Get top leagues by match count
    top_leagues = df['league'].value_counts().head(top_n).index.tolist()
    filtered_df = df[df['league'].isin(top_leagues)]
    
    # Create pivot table
    pivot = pd.pivot_table(
        filtered_df,
        values='id',
        index='league',
        columns='day_of_week',
        aggfunc='count',
        fill_value=0
    )
    
    # Reorder days
    day_columns = [day for day in day_order if day in pivot.columns]
    pivot = pivot[day_columns]
    
    # Create heatmap with Apple-style colors
    fig = px.imshow(
        pivot,
        title=None,
        labels=dict(x="", y="", color="Matches"),
        color_continuous_scale=['#f5f5f7', '#0071e3'],
        template="plotly_white",
        aspect="auto",
        text_auto=True
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        coloraxis_colorbar=dict(
            title=None,
            tickfont=dict(size=12)
        ),
        margin=dict(t=0, b=50, l=150, r=50),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        height=500
    )
    
    # Update text style
    fig.update_traces(
        texttemplate="%{z}",
        textfont=dict(
            family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif",
            size=12,
            color="black"
        )
    )
    
    return fig

def create_team_comparison_chart(df, team1, team2):
    """
    Create an Apple-style comparison chart for two teams
    
    Args:
        df: DataFrame with match data
        team1: First team name
        team2: Second team name
        
    Returns:
        Plotly figure
    """
    team1_home = df[df['home_team'] == team1]
    team1_away = df[df['away_team'] == team1]
    team1_total = len(team1_home) + len(team1_away)
    
    team2_home = df[df['home_team'] == team2]
    team2_away = df[df['away_team'] == team2]
    team2_total = len(team2_home) + len(team2_away)
    
    # Prepare data for the chart
    data = [
        {'Team': team1, 'Matches': len(team1_home), 'Type': 'Home'},
        {'Team': team1, 'Matches': len(team1_away), 'Type': 'Away'},
        {'Team': team2, 'Matches': len(team2_home), 'Type': 'Home'},
        {'Team': team2, 'Matches': len(team2_away), 'Type': 'Away'}
    ]
    
    df_comp = pd.DataFrame(data)
    
    # Create grouped bar chart with Apple-style colors
    fig = px.bar(
        df_comp,
        x='Team',
        y='Matches',
        color='Type',
        title=None,
        barmode='group',
        color_discrete_map={'Home': '#0071e3', 'Away': '#76d0ff'},
        template="plotly_white",
        text_auto=True
    )
    
    # Add total annotation
    annotations = [
        dict(
            x=team1,
            y=team1_total + 1,
            text=f"Total: {team1_total}",
            showarrow=False,
            font=dict(
                family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif",
                size=14
            )
        ),
        dict(
            x=team2,
            y=team2_total + 1,
            text=f"Total: {team2_total}",
            showarrow=False,
            font=dict(
                family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif",
                size=14
            )
        )
    ]
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=None,
        legend_title=None,
        margin=dict(t=40, b=20, l=20, r=20),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            tickfont=dict(size=14),
            gridcolor='rgba(0,0,0,0.05)'
        ),
        yaxis=dict(
            gridcolor='rgba(0,0,0,0.05)'
        ),
        annotations=annotations,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Update bar styles
    fig.update_traces(
        marker_line_width=0,
        textposition='outside'
    )
    
    return fig

def create_match_timeline(df, days_range=7):
    """
    Create an Apple-style timeline chart showing matches distribution by hour of day
    
    Args:
        df: DataFrame with match data
        days_range: Number of days to look ahead
        
    Returns:
        Plotly figure
    """
    # Ensure date and start_time columns exist
    if 'date' not in df.columns or 'start_time' not in df.columns:
        return None
    
    if not isinstance(df['date'].iloc[0], pd.Timestamp):
        df['date'] = pd.to_datetime(df['date'])
    
    # Filter to only include matches within the next N days
    today = pd.Timestamp.now().floor('D')
    future_df = df[(df['date'] >= today) & (df['date'] <= today + pd.Timedelta(days=days_range))]
    
    if future_df.empty:
        return None
    
    # Extract hour from start_time
    future_df['hour'] = future_df['start_time'].str.extract(r'(\d+):').astype(int)
    
    # Count matches by hour
    hour_counts = future_df.groupby('hour').size().reset_index()
    hour_counts.columns = ['hour', 'count']
    
    # Fill in missing hours
    all_hours = pd.DataFrame({'hour': range(0, 24)})
    hour_counts = all_hours.merge(hour_counts, on='hour', how='left').fillna(0)
    
    # Create line chart with Apple-style colors
    fig = go.Figure()
    
    # Add area under the line
    fig.add_trace(
        go.Scatter(
            x=hour_counts['hour'],
            y=hour_counts['count'],
            fill='tozeroy',
            fillcolor='rgba(0, 113, 227, 0.2)',
            line=dict(color='#0071e3', width=3),
            mode='lines+markers',
            name='',
            marker=dict(
                size=8,
                color='#0071e3',
                line=dict(width=1, color='white')
            ),
            hovertemplate='<b>%{x}:00</b><br>Matches: %{y}<extra></extra>'
        )
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        title=None,
        xaxis=dict(
            title=None,
            tickmode='linear',
            tick0=0,
            dtick=2,
            ticktext=[f"{h:02d}:00" for h in range(0, 24, 2)],
            tickvals=list(range(0, 24, 2)),
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'
        ),
        yaxis=dict(
            title=None,
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'
        ),
        margin=dict(t=0, b=20, l=20, r=20),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        hoverlabel=dict(bgcolor="white", font_size=14),
        showlegend=False
    )
    
    return fig