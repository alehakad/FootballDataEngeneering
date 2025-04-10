import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define dataset
analytics_dataset_id = "footballdataengineering.analytics"

# Create position-to-table mapping
position_table_mapping = {
    "CB": "radar_metrics_centerbacks_seasons",
    "FB": "radar_metrics_fullbacks_seasons",
    "CM": "radar_metrics_centralmidfielders_seasons",
    "AM": "radar_metrics_attackingmidfielders_seasons",
    "FW": "radar_metrics_forwards_seasons"
    # Add more positions and their corresponding tables as needed
}

# Set Streamlit title
st.title("⚽ Player Stats Scatter Explorer")

# Create sidebar for filters
st.sidebar.header("Filters")

# Position selection dropdown in sidebar
selected_position = st.sidebar.selectbox("Select Position", list(position_table_mapping.keys()))

# Get the appropriate table based on selected position
selected_table = position_table_mapping[selected_position]

# Try to join with player dimension table to get player names
try:
    # First, check if we can join with a dimension table to get player names
    df = client.query(f"""
        SELECT m.*, p.player_name 
        FROM `{analytics_dataset_id}.{selected_table}` m
        LEFT JOIN `{analytics_dataset_id}.dim_players` p ON m.player_id = p.player_id
    """).to_dataframe()
    
    has_player_names = 'player_name' in df.columns and not df['player_name'].isna().all()
    
    # Check if we actually got data
    if df.empty:
        st.error(f"No data found for position {selected_position} in table {selected_table}")
        st.stop()
        
except Exception as e:
    # If the join fails, try without the join
    st.warning(f"Could not join with player dimension table: {str(e)}")
    st.warning("Displaying player IDs instead of names. To see player names, make sure a dim_players table exists with player_id and player_name columns.")
    
    try:
        df = client.query(f"""
            SELECT * FROM `{analytics_dataset_id}.{selected_table}`
        """).to_dataframe()
        
        # Create a player_name column using player_id if it doesn't exist
        if 'player_name' not in df.columns and 'player_id' in df.columns:
            df['player_name'] = df['player_id'].astype(str)
        
        has_player_names = False
        
        # Check if we actually got data
        if df.empty:
            st.error(f"No data found for position {selected_position} in table {selected_table}")
            st.stop()
            
    except Exception as e2:
        st.error(f"Error querying table for position {selected_position}: {str(e2)}")
        st.stop()

# Season selection dropdown in sidebar
all_seasons_option = "All Seasons"
if 'season_id' in df.columns:
    season_options = [all_seasons_option] + sorted(df['season_id'].dropna().unique().tolist())
    selected_season = st.sidebar.selectbox("Select Season", season_options)

    # Filter data based on season selection
    if selected_season != all_seasons_option:
        filtered_df = df[df['season_id'] == selected_season]
    else:
        filtered_df = df
else:
    st.warning("No season_id column found in the data")
    filtered_df = df

# Add a minimum minutes or games filter if appropriate columns exist
filter_applied = False
if 'minutes_played' in filtered_df.columns:
    min_minutes = st.sidebar.slider("Minimum Minutes Played", 0, 5000, 500, 100)
    filtered_df = filtered_df[filtered_df['minutes_played'] >= min_minutes]
    filter_applied = True
elif 'games_played' in filtered_df.columns:
    min_games = st.sidebar.slider("Minimum Games Played", 0, 50, 5, 1)
    filtered_df = filtered_df[filtered_df['games_played'] >= min_games]
    filter_applied = True

# Identify ID columns to exclude
typical_id_columns = ['player_id', 'game_id', 'season_id', 'match_id', 'team_id']
exclude_columns = [col for col in filtered_df.columns if any(id_col in col.lower() for id_col in ['id', 'name', 'date'])]
exclude_columns.extend([col for col in typical_id_columns if col in filtered_df.columns])

# Get numeric columns as potential metrics
numeric_columns = filtered_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
metrics = [col for col in numeric_columns if col not in exclude_columns]

if not metrics:
    st.error(f"No numeric metrics found for position {selected_position} in table {selected_table}")
    st.stop()

# Let the user choose two features to compare
x_col = st.selectbox("Select X-axis metric", metrics, index=0)
y_col = st.selectbox("Select Y-axis metric", metrics, index=min(1, len(metrics)-1) if len(metrics) > 1 else 0)

# Filter out rows with nulls in selected metrics
hover_columns = ['player_name', 'player_id', 'season_id', 'team_id']
available_hover_columns = [col for col in hover_columns if col in filtered_df.columns]
plot_df = filtered_df[[x_col, y_col] + available_hover_columns].dropna(subset=[x_col, y_col])

# Show number of players displayed
st.write(f"Displaying data for {len(plot_df)} players")

# Build scatter plot with player names
fig = px.scatter(
    plot_df,
    x=x_col,
    y=y_col,
    hover_data=available_hover_columns,
    text='player_name' if 'player_name' in plot_df.columns else 'player_id',
    title=f"{selected_position}: {x_col.replace('_', ' ').title()} vs {y_col.replace('_', ' ').title()}",
    color='season_id' if 'season_id' in plot_df.columns and selected_season == all_seasons_option else None,
)

# Adjust text position to appear near the points
fig.update_traces(
    textposition='top center',
    textfont=dict(
        size=10,
    )
)

# Add information about the selected position and season to the title
title = f"{selected_position}: {x_col.replace('_', ' ').title()} vs {y_col.replace('_', ' ').title()}"
if 'season_id' in df.columns and selected_season != all_seasons_option:
    title += f" - Season {selected_season}"
fig.update_layout(title=title)

st.plotly_chart(fig, use_container_width=True)

# Add an option to show the data table
if st.checkbox("Show data table"):
    # Select columns to display
    display_cols = ['player_name' if 'player_name' in plot_df.columns else 'player_id', x_col, y_col]
    for col in ['season_id', 'team_id', 'minutes_played', 'games_played']:
        if col in plot_df.columns:
            display_cols.append(col)
    
    st.dataframe(plot_df[display_cols])

# Optional: Add a note about player name mapping if using player IDs
if not has_player_names:
    st.markdown("""
    **Note:** This visualization currently shows player IDs. To display player names, 
    you'll need to add a dimension table named `dim_players` with `player_id` and `player_name` columns.
    """)