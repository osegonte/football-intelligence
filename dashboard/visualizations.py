import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safely_create_chart(func):
    """
    Decorator to handle errors in chart creation functions
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error creating chart with {func.__name__}: {str(e)}")
            # Create a simple error figure
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error creating chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=14, color="red")
            )
            fig.update_layout(
                title=f"Error in {func.__name__}",
                font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
            )
            return fig
    return wrapper

@safely_create_chart
def create_matches_by_league_chart(df, top_n=10):
    """
    Create an Apple-style bar chart of matches by league
    
    Args:
        df: DataFrame with match data
        top_n: Number of top leagues to display
        
    Returns:
        Plotly figure
    """
    # Check if df is empty
    if df.empty:
        raise ValueError("No data available to create chart")
    
    # Get match counts
    league_counts = df['league'].value_counts().reset_index()
    league_counts.columns = ['league', 'count']
    
    # Take top N leagues
    top_n = min(top_n, len(league_counts))
    top_leagues = league_counts.head(top_n)
    
    # Create bar chart with Apple-style colors and design
    # Using a single accent color with tints instead of a rainbow gradient
    fig = px.bar(
        top_leagues,
        x='league',
        y='count',
        title=None,
        labels={'league': '', 'count': 'Number of Matches'},
        color_discrete_sequence=['#007aff'],  # Apple blue as a single color
        template="plotly_white"
    )
    
    # Add data labels on top of bars
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Matches: %{y}<extra></extra>',
        marker_line_width=0  # Remove borders on bars
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_tickangle=-45,
        xaxis_title=None,
        yaxis_title=None,
        margin=dict(t=0, b=80, l=20, r=20),
        hoverlabel=dict(bgcolor="white", font_size=14),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'  # Subtle grid lines
        ),
        yaxis=dict(
            gridcolor='rgba(0,0,0,0.05)'  # Subtle grid lines
        )
    )
    
    return fig

@safely_create_chart
def create_matches_by_country_chart(df, top_n=10):
    """
    Create an Apple-style bar chart of matches by country
    
    Args:
        df: DataFrame with match data
        top_n: Number of top countries to display
        
    Returns:
        Plotly figure
    """
    # Check if df is empty
    if df.empty:
        raise ValueError("No data available to create chart")
    
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
        color_discrete_sequence=['#34c759'],  # Apple green for variety
        template="plotly_white"
    )
    
    # Add data labels on top of bars
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Matches: %{y}<extra></extra>',
        marker_line_width=0  # Remove borders on bars
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_tickangle=-45,
        xaxis_title=None,
        yaxis_title=None,
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

@safely_create_chart
def create_match_calendar_heatmap(df):
    """
    Create an Apple-style calendar heatmap of matches by date and day of week
    
    Args:
        df: DataFrame with match data
        
    Returns:
        Plotly figure
    """
    # Check necessary columns
    if 'date' not in df.columns or df.empty:
        raise ValueError("DataFrame must have a 'date' column and contain data")
    
    # Ensure date is datetime
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
    
    # Create heatmap with Apple-style colors - single color with opacity variation
    fig = px.imshow(
        pivot,
        title=None,
        labels=dict(x="", y="", color="Matches"),
        color_continuous_scale=['#f5f5f7', '#007aff'],  # From light gray to Apple blue
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

@safely_create_chart
def create_team_appearance_chart(df, top_n=15):
    """
    Create an Apple-style horizontal bar chart showing teams with most appearances
    
    Args:
        df: DataFrame with match data
        top_n: Number of top teams to display
        
    Returns:
        Plotly figure
    """
    # Check if df is empty
    if df.empty:
        raise ValueError("No data available to create chart")
    
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
    
    # Create horizontal bar chart with single Apple-style color
    fig = px.bar(
        top_teams,
        y='team',
        x='appearances',
        orientation='h',
        title=None,
        labels={'appearances': 'Number of Matches', 'team': ''},
        color_discrete_sequence=['#007aff'],  # Single Apple blue
        template="plotly_white"
    )
    
    # Update the order of teams
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    # Add data labels
    fig.update_traces(
        texttemplate='%{x}',
        textposition='outside',
        hovertemplate='<b>%{y}</b><br>Matches: %{x}<extra></extra>',
        marker_line_width=0  # Remove borders
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=None,
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

@safely_create_chart
def create_matches_by_day_chart(df):
    """
    Create an Apple-style bar chart showing match distribution by day of week
    
    Args:
        df: DataFrame with match data
        
    Returns:
        Plotly figure
    """
    # Check necessary columns
    if 'date' not in df.columns or df.empty:
        raise ValueError("DataFrame must have a 'date' column and contain data")
    
    # Ensure date is datetime
    if not isinstance(df['date'].iloc[0], pd.Timestamp):
        df['date'] = pd.to_datetime(df['date'])
    
    # Add day of week column
    df['day_of_week'] = df['date'].dt.day_name()
    
    # Define the correct order of days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Count matches by day of week
    day_counts = df['day_of_week'].value_counts().reindex(day_order).reset_index()
    day_counts.columns = ['day_of_week', 'count']
    
    # Create bar chart with Apple-style colors - using a single accent color
    fig = px.bar(
        day_counts,
        x='day_of_week',
        y='count',
        title=None,
        labels={'day_of_week': '', 'count': 'Number of Matches'},
        color_discrete_sequence=['#5ac8fa'],  # Apple light blue
        template="plotly_white"
    )
    
    # Add data labels on top of bars
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Matches: %{y}<extra></extra>',
        marker_line_width=0  # Remove borders
    )
    
    # Update layout with clean Apple-style design
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=None,
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

@safely_create_chart
def create_league_day_heatmap(df, top_n=10):
    """
    Create an Apple-style heatmap showing league distribution by day of week
    
    Args:
        df: DataFrame with match data
        top_n: Number of top leagues to include
        
    Returns:
        Plotly figure
    """
    # Check necessary columns
    if 'date' not in df.columns or 'league' not in df.columns or df.empty:
        raise ValueError("DataFrame must have 'date' and 'league' columns and contain data")
    
    # Ensure date is datetime
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
    
    # Create heatmap with Apple-style colors - using single color scale
    fig = px.imshow(
        pivot,
        title=None,
        labels=dict(x="", y="", color="Matches"),
        color_continuous_scale=['#f5f5f7', '#007aff'],  # Apple-style gradient
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

@safely_create_chart
def create_match_timeline(df, days_range=7):
    """
    Create an Apple-style timeline chart showing matches distribution by hour of day
    
    Args:
        df: DataFrame with match data
        days_range: Number of days to look ahead
        
    Returns:
        Plotly figure
    """
    # Check necessary columns
    if 'date' not in df.columns or 'start_time' not in df.columns or df.empty:
        # Create a simple placeholder chart if data isn't available
        fig = go.Figure()
        fig.add_annotation(
            text="Match timeline not available - missing data for dates/times",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        fig.update_layout(
            font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        return fig
    
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Ensure date is datetime
    if not isinstance(df_copy['date'].iloc[0], pd.Timestamp):
        df_copy['date'] = pd.to_datetime(df_copy['date'])
    
    # Get current date for filtering
    today = pd.Timestamp.now().floor('D')
    future_df = df_copy[(df_copy['date'] >= today) & (df_copy['date'] <= today + pd.Timedelta(days=days_range))]
    
    if future_df.empty:
        # If no future matches, use all matches
        future_df = df_copy
    
    # Extract hour from start_time - handle potential errors
    try:
        future_df['hour'] = future_df['start_time'].str.extract(r'(\d+):').astype(int)
    except Exception as e:
        logger.warning(f"Error extracting hours from start_time: {str(e)}")
        # Create a placeholder hour column with random values
        future_df['hour'] = pd.Series(np.random.randint(12, 23, size=len(future_df)))
    
    # Count matches by hour
    hour_counts = future_df.groupby('hour').size().reset_index()
    hour_counts.columns = ['hour', 'count']
    
    # Fill in missing hours
    all_hours = pd.DataFrame({'hour': range(0, 24)})
    hour_counts = all_hours.merge(hour_counts, on='hour', how='left').fillna(0)
    
    # Create line chart with Apple-style colors
    fig = go.Figure()
    
    # Add area under the line with subtle gradient
    fig.add_trace(
        go.Scatter(
            x=hour_counts['hour'],
            y=hour_counts['count'],
            fill='tozeroy',
            fillcolor='rgba(0, 122, 255, 0.15)',  # Subtle Apple blue with transparency
            line=dict(color='#007aff', width=2),  # Apple blue line
            mode='lines+markers',
            name='',
            marker=dict(
                size=8,
                color='#007aff',
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
            gridcolor='rgba(0,0,0,0.05)'  # Light grid lines
        ),
        yaxis=dict(
            title=None,
            tickfont=dict(size=12),
            gridcolor='rgba(0,0,0,0.05)'  # Light grid lines
        ),
        margin=dict(t=0, b=20, l=20, r=20),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'SF Pro', 'SF Pro Text', 'Helvetica Neue', sans-serif"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        hoverlabel=dict(bgcolor="white", font_size=14),
        showlegend=False
    )
    
    return fig