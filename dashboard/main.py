import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

# Streamlit page configuration
st.set_page_config(
    page_title="Football Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Cache data loading functions to improve performance
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_positions():
    try:
        bq_client = bigquery.Client()
        query = """
            SELECT position_id, category
            FROM `footballdataengineering.analytics.dim_positions`
        """
        df = bq_client.query(query).to_dataframe()
        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_players_with_details():
    try:
        bq_client = bigquery.Client()
        query = """
            SELECT player_id, player_name, birth_date, nationality
            FROM `footballdataengineering.analytics.dim_players`
            ORDER BY player_name
        """
        df = bq_client.query(query).to_dataframe()
        # Clean up the nationality field if it contains nested JSON/dict
        if 'nationality' in df.columns:
            df['nationality'] = df['nationality'].apply(
                lambda x: x['element'] if isinstance(x, dict) and 'element' in x else
                x[0]['element'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) and 'element' in x[
                    0] else
                x
            )
        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_players_by_position(position_category):
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT DISTINCT
                p.player_id, 
                p.player_name, 
                p.birth_date, 
                p.nationality
            FROM `footballdataengineering.analytics.dim_players` p
            JOIN `footballdataengineering.analytics.fact_player_season` fps
                ON p.player_id = fps.player_id
            WHERE fps.position_category = '{position_category}'
            ORDER BY p.player_name
        """
        df = bq_client.query(query).to_dataframe()
        # Clean up the nationality field if it contains nested JSON/dict
        if 'nationality' in df.columns:
            df['nationality'] = df['nationality'].apply(
                lambda x: x['element'] if isinstance(x, dict) and 'element' in x else
                x[0]['element'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) and 'element' in x[
                    0] else
                x
            )
        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_teams():
    try:
        bq_client = bigquery.Client()
        query = """
            SELECT team_id, team_name
            FROM `footballdataengineering.analytics.dim_teams`
            ORDER BY team_name
        """
        df = bq_client.query(query).to_dataframe()
        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_seasons():
    try:
        bq_client = bigquery.Client()
        query = """
            SELECT DISTINCT season_id
            FROM `footballdataengineering.analytics.radar_metrics_centerbacks_seasons`
            ORDER BY season_id
        """
        df = bq_client.query(query).to_dataframe()
        return df["season_id"].tolist(), None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_players_by_team_position_season(team_id, position_category, season_id):
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT 
                p.player_id, 
                p.player_name, 
                p.birth_date, 
                p.nationality
            FROM `footballdataengineering.analytics.dim_players` p
            JOIN `footballdataengineering.analytics.fact_player_season` fps
                ON p.player_id = fps.player_id
            WHERE 
                fps.team_id = {team_id} 
                AND fps.position_category = '{position_category}'
                AND fps.season_id = {season_id}
            ORDER BY p.player_name
        """
        df = bq_client.query(query).to_dataframe()
        # Clean up the nationality field if it contains nested JSON/dict
        if 'nationality' in df.columns:
            df['nationality'] = df['nationality'].apply(
                lambda x: x['element'] if isinstance(x, dict) and 'element' in x else
                x[0]['element'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) and 'element' in x[
                    0] else
                x
            )
        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_player_career_data(player_id):
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT 
                player_id,
                aerial_duels_won_percentile,
                clearences_percentile,
                interceptions_percentile,
                blocks_percentile,
                progressive_passes_percentile,
                long_pass_completion_percentile
            FROM `footballdataengineering.analytics.radar_metrics_centerbacks_career`
            WHERE player_id = {player_id}
        """
        df = bq_client.query(query).to_dataframe()
        # Convert values to numeric to ensure they are valid for the radar chart
        numeric_cols = [
            'aerial_duels_won_percentile', 'clearences_percentile',
            'interceptions_percentile', 'blocks_percentile',
            'progressive_passes_percentile', 'long_pass_completion_percentile'
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_player_season_data(player_id, season_id):
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT 
                player_id, 
                season_id,
                aerial_duels_won_percentile,
                clearences_percentile,
                interceptions_percentile,
                blocks_percentile,
                progressive_passes_percentile,
                long_pass_completion_percentile
            FROM `footballdataengineering.analytics.radar_metrics_centerbacks_seasons`
            WHERE player_id = {player_id} AND season_id = {season_id}
        """
        df = bq_client.query(query).to_dataframe()
        # Convert values to numeric to ensure they are valid for the radar chart
        numeric_cols = [
            'aerial_duels_won_percentile', 'clearences_percentile',
            'interceptions_percentile', 'blocks_percentile',
            'progressive_passes_percentile', 'long_pass_completion_percentile'
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df, None
    except Exception as e:
        return None, str(e)


def create_radar_chart(df, player_name=None):
    if df is None or df.empty:
        return None

    # Get the metrics for the radar chart
    categories = [
        'Aerial Duels Won',
        'Clearances',
        'Interceptions',
        'Blocks',
        'Progressive Passes',
        'Long Pass Completion'
    ]

    # Convert values to Python native float to avoid numpy type issues
    values = [
        float(df['aerial_duels_won_percentile'].iloc[0]) if not pd.isna(
            df['aerial_duels_won_percentile'].iloc[0]) else 0.0,
        float(df['clearences_percentile'].iloc[0]) if not pd.isna(df['clearences_percentile'].iloc[0]) else 0.0,
        float(df['interceptions_percentile'].iloc[0]) if not pd.isna(df['interceptions_percentile'].iloc[0]) else 0.0,
        float(df['blocks_percentile'].iloc[0]) if not pd.isna(df['blocks_percentile'].iloc[0]) else 0.0,
        float(df['progressive_passes_percentile'].iloc[0]) if not pd.isna(
            df['progressive_passes_percentile'].iloc[0]) else 0.0,
        float(df['long_pass_completion_percentile'].iloc[0]) if not pd.isna(
            df['long_pass_completion_percentile'].iloc[0]) else 0.0
    ]

    # Close the loop for the radar
    categories = categories + [categories[0]]
    values = values + [values[0]]

    fig = go.Figure()

    # Use player name if provided, otherwise use player_id
    player_id_str = str(df['player_id'].iloc[0])
    display_name = player_name if player_name else f"Player ID: {player_id_str}"

    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=display_name,
        line_color='blue',
        fillcolor='rgba(0, 0, 255, 0.3)'
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )
        ),
        showlegend=True,
        title=f"Radar Chart for {display_name}"
    )

    return fig


# App title and description
st.title("Football Analytics Dashboard")

# Main content - Player Radar Charts with Position Selection
st.header("Player Radar Charts")

# Load base data - this is needed regardless of user choices
with st.spinner("Loading base data..."):
    positions_df, positions_error = load_positions()
    teams_df, teams_error = load_teams()
    seasons, season_error = load_seasons()

if positions_error:
    st.error(f"Error loading positions: {positions_error}")
elif teams_error:
    st.error(f"Error loading teams: {teams_error}")
elif season_error:
    st.error(f"Error loading seasons: {season_error}")
elif positions_df is not None and teams_df is not None and seasons is not None:
    # Data type selection
    data_type = st.radio(
        "Select data type:",
        ["Career", "Season"],
        horizontal=True
    )

    # Position selection
    categories = positions_df["category"].unique().tolist()
    selected_category = st.selectbox(
        "Select Position Category:",
        options=categories,
        index=0
    )

    # Create columns for the remaining filters
    col1, col2 = st.columns(2)

    selected_player_id = None
    selected_player_name = None
    selected_player_details = None

    # Different filtering logic based on data type
    if data_type == "Career":
        with col1:
            # Load players filtered by position
            with st.spinner(f"Loading {selected_category} players..."):
                position_players_df, position_players_error = load_players_by_position(selected_category)

            if position_players_error:
                st.error(f"Error loading players: {position_players_error}")
            elif position_players_df is not None and not position_players_df.empty:
                # Player selection for career data, filtered by position
                player_options = position_players_df["player_name"].tolist()
                selected_player_name = st.selectbox(
                    "Select Player:",
                    options=player_options,
                    index=0
                )

                # Get the selected player details
                selected_player_row = position_players_df[position_players_df["player_name"] == selected_player_name]
                if not selected_player_row.empty:
                    selected_player_id = int(selected_player_row["player_id"].iloc[0])
                    selected_player_details = selected_player_row

                    # Display player details
                    st.write("Player Details:")
                    nationality_raw = selected_player_row['nationality'].iloc[0]
                    nationalities = [item['element'] for item in nationality_raw['list']]
                    st.write(f"- Nationality: {', '.join(nationalities)}")
                    st.write(f"- Birth Date: {selected_player_row['birth_date'].iloc[0]}")
            else:
                st.warning(f"No players found for position category: {selected_category}")

    else:  # Season data
        with col1:
            # Season selection for season data
            selected_season = st.selectbox(
                "Select Season:",
                options=seasons,
                index=0
            )

            # Team selection for season data
            team_options = teams_df["team_name"].tolist()
            selected_team_name = st.selectbox(
                "Select Team:",
                options=team_options,
                index=0
            )

            # Get the selected team ID
            selected_team_row = teams_df[teams_df["team_name"] == selected_team_name]
            if not selected_team_row.empty:
                selected_team_id = int(selected_team_row["team_id"].iloc[0])

                # Load players for selected team, position category, and season
                with st.spinner(f"Loading team players..."):
                    team_players_df, team_player_error = load_players_by_team_position_season(
                        selected_team_id, selected_category, selected_season)

                if team_player_error:
                    st.error(f"Error loading team players: {team_player_error}")
                elif team_players_df is not None and not team_players_df.empty:
                    # Player selection from team
                    player_options = team_players_df["player_name"].tolist()
                    selected_player_name = st.selectbox(
                        "Select Player from Team:",
                        options=player_options,
                        index=0 if player_options else None
                    )

                    # Get the selected player details
                    if selected_player_name:
                        selected_player_row = team_players_df[team_players_df["player_name"] == selected_player_name]
                        if not selected_player_row.empty:
                            selected_player_id = int(selected_player_row["player_id"].iloc[0])
                            selected_player_details = selected_player_row

                            # Display player details
                            st.write("Player Details:")
                            nationality_raw = selected_player_row['nationality'].iloc[0]
                            nationalities = [item['element'] for item in nationality_raw['list']]
                            st.write(f"- Nationality: {', '.join(nationalities)}")
                            st.write(f"- Birth Date: {selected_player_row['birth_date'].iloc[0]}")
                else:
                    st.warning(f"No players found for selected team and position in this season.")

    # Load and display player data
    if st.button("Generate Radar Chart"):
        if selected_player_id is None:
            st.warning("Please select a player.")
        else:
            with st.spinner("Loading player statistics..."):
                if data_type == "Career":
                    player_data, data_error = load_player_career_data(selected_player_id)
                else:  # Season
                    player_data, data_error = load_player_season_data(selected_player_id, selected_season)

            if data_error:
                st.error(f"Error loading player data: {data_error}")
            elif player_data is None or player_data.empty:
                st.warning(f"No {data_type.lower()} data available for {selected_player_name}.")
            else:
                # Display the metric data
                if data_type == "Career":
                    display_df = player_data.drop(columns=["player_id"])
                else:
                    display_df = player_data.drop(columns=["player_id", "season_id"])
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                # Create and display radar chart with player name
                chart = create_radar_chart(player_data, selected_player_name)
                if chart:
                    st.plotly_chart(chart, use_container_width=True)

                # Add download button for the player data
                st.download_button(
                    label="Download player data as CSV",
                    data=player_data.to_csv(index=False).encode('utf-8'),
                    file_name=f"{selected_player_name}_{data_type.lower()}.csv",
                    mime='text/csv',
                )

# Footer with app information
st.sidebar.markdown("---")
st.sidebar.info("Football Analytics Dashboard")
st.sidebar.text(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
