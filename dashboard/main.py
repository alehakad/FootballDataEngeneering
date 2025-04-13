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

# Define the database prefix (project.dataset)
DB_PREFIX = "footballdataengineering.analytics"


@st.cache_data(ttl=3600)
def load_multiple_player_career_data(player_ids, radar_metrics_table):
    try:
        bq_client = bigquery.Client()

        # Get the metrics columns dynamically
        query_metadata = f"""
            SELECT column_name
            FROM `{DB_PREFIX}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{radar_metrics_table}_career'
            AND column_name LIKE '%_percentile'
        """

        metric_cols = bq_client.query(query_metadata).to_dataframe()

        if metric_cols.empty:
            return None, f"No percentile metrics found for table {radar_metrics_table}_career"

        # Build the query dynamically
        metric_cols_str = ", ".join(metric_cols['column_name'].tolist())

        # Convert player_ids to a comma-separated string for the IN clause
        player_ids_str = ", ".join([str(pid) for pid in player_ids])

        query = f"""
            SELECT 
                p.player_id,
                p.player_name,
                {metric_cols_str}
            FROM `{DB_PREFIX}.{radar_metrics_table}_career` m
            JOIN `{DB_PREFIX}.dim_players` p ON m.player_id = p.player_id
            WHERE m.player_id IN ({player_ids_str})
        """

        df = bq_client.query(query).to_dataframe()

        # Convert values to numeric to ensure they are valid for the radar chart
        for col in metric_cols['column_name'].tolist():
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_multiple_player_season_data(player_ids, season_id, radar_metrics_table):
    try:
        bq_client = bigquery.Client()

        # Get the metrics columns dynamically
        query_metadata = f"""
            SELECT column_name
            FROM `{DB_PREFIX}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{radar_metrics_table}_seasons'
            AND column_name LIKE '%_percentile'
        """

        metric_cols = bq_client.query(query_metadata).to_dataframe()

        if metric_cols.empty:
            return None, f"No percentile metrics found for table {radar_metrics_table}_seasons"

        # Build the query dynamically
        metric_cols_str = ", ".join(metric_cols['column_name'].tolist())

        # Convert player_ids to a comma-separated string for the IN clause
        player_ids_str = ", ".join([str(pid) for pid in player_ids])

        query = f"""
            SELECT 
                p.player_id,
                p.player_name,
                m.season_id,
                {metric_cols_str}
            FROM `{DB_PREFIX}.{radar_metrics_table}_seasons` m
            JOIN `{DB_PREFIX}.dim_players` p ON m.player_id = p.player_id
            WHERE m.player_id IN ({player_ids_str}) AND m.season_id = '{season_id}'
        """

        df = bq_client.query(query).to_dataframe()

        # Convert values to numeric to ensure they are valid for the radar chart
        for col in metric_cols['column_name'].tolist():
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df, None
    except Exception as e:
        return None, str(e)


# New function to load similar players
@st.cache_data(ttl=3600)
def load_similar_players(player_id, position_category, limit=5):
    try:
        bq_client = bigquery.Client()

        # Convert position_category to table name format (e.g., "Striker" -> "strikers_similarity")
        similarity_table = f"{position_category.lower().replace(' ', '_')}s_similarity"

        # Query to get similar players
        query = f"""
            SELECT 
                p.player_name,
                s.similarity_score,
                p.birth_date,
                p.nationality
            FROM `{DB_PREFIX}.{similarity_table}` s
            JOIN `{DB_PREFIX}.dim_players` p ON s.similar_player_id = p.player_id
            WHERE s.player_id = {player_id}
            ORDER BY s.similarity_score DESC
            LIMIT {limit}
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


# Cache data loading functions to improve performance
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_positions():
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT position_id, category, radar_metrics_table
            FROM `{DB_PREFIX}.dim_positions`
        """
        df = bq_client.query(query).to_dataframe()
        return df, None
    except Exception as e:
        return None, str(e)


def create_radar_chart_multiple(df, metric_cols=None):
    if df is None or df.empty:
        return None

    # Get only the percentile metrics columns if not provided
    if metric_cols is None:
        metric_cols = [col for col in df.columns if col.endswith('_percentile')]

    if not metric_cols:
        return None

    # Create display names for the metrics
    categories = [col.replace('_percentile', '').replace('_', ' ').title() for col in metric_cols]

    # Define a color palette for multiple players
    colors = ['blue', 'red', 'green', 'purple', 'orange', 'cyan', 'magenta', 'yellow']

    fig = go.Figure()

    # Group by player
    for i, (player_id, player_data) in enumerate(df.groupby('player_id')):
        if i >= len(colors):  # Limit to available colors
            break

        # Get values for this player
        values = [float(player_data[col].iloc[0]) if not pd.isna(player_data[col].iloc[0]) else 0.0 for col in
                  metric_cols]

        # Close the loop for the radar
        player_categories = categories + [categories[0]]
        player_values = values + [values[0]]

        player_name = player_data['player_name'].iloc[0]

        fig.add_trace(go.Scatterpolar(
            r=player_values,
            theta=player_categories,
            fill='toself',
            name=player_name,
            line_color=colors[i],
            fillcolor=f'rgba({",".join([str(int(colors[i] == c) * 255) for c in ["blue", "red", "green"]])}, 0.3)'
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )
        ),
        showlegend=True,
        title="Player Comparison Radar Chart"
    )

    return fig


@st.cache_data(ttl=3600)
def load_players_with_details():
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT player_id, player_name, birth_date, nationality
            FROM `{DB_PREFIX}.dim_players`
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
            FROM `{DB_PREFIX}.dim_players` p
            JOIN `{DB_PREFIX}.fact_player_season` fps
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
        query = f"""
            SELECT team_id, team_name
            FROM `{DB_PREFIX}.dim_teams`
            ORDER BY team_name
        """
        df = bq_client.query(query).to_dataframe()
        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_seasons(radar_metrics_table):
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT DISTINCT season_id
            FROM `{DB_PREFIX}.{radar_metrics_table}_seasons`
            ORDER BY season_id
        """
        df = bq_client.query(query).to_dataframe()
        return df["season_id"].tolist(), None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_players_by_team_position_season(team_id: int, position_category: str, season_id: str):
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT 
                p.player_id, 
                p.player_name, 
                p.birth_date, 
                p.nationality
            FROM `{DB_PREFIX}.dim_players` p
            JOIN `{DB_PREFIX}.fact_player_season` fps
                ON p.player_id = fps.player_id
            WHERE 
                fps.team_id = {team_id} 
                AND fps.position_category = '{position_category}'
                AND fps.season_id = '{season_id}'
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
def load_player_career_data(player_id, radar_metrics_table):
    try:
        bq_client = bigquery.Client()

        # Get the metrics columns dynamically
        query_metadata = f"""
            SELECT column_name
            FROM `{DB_PREFIX}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{radar_metrics_table}_career'
            AND column_name LIKE '%_percentile'
        """

        metric_cols = bq_client.query(query_metadata).to_dataframe()

        if metric_cols.empty:
            return None, f"No percentile metrics found for table {radar_metrics_table}_career"

        # Build the query dynamically
        metric_cols_str = ", ".join(metric_cols['column_name'].tolist())

        query = f"""
            SELECT 
                player_id,
                {metric_cols_str}
            FROM `{DB_PREFIX}.{radar_metrics_table}_career`
            WHERE player_id = {player_id}
        """

        df = bq_client.query(query).to_dataframe()

        # Convert values to numeric to ensure they are valid for the radar chart
        for col in metric_cols['column_name'].tolist():
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df, None
    except Exception as e:
        return None, str(e)


@st.cache_data(ttl=3600)
def load_player_season_data(player_id, season_id, radar_metrics_table):
    try:
        bq_client = bigquery.Client()

        # Get the metrics columns dynamically
        query_metadata = f"""
            SELECT column_name
            FROM `{DB_PREFIX}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{radar_metrics_table}_seasons'
            AND column_name LIKE '%_percentile'
        """

        metric_cols = bq_client.query(query_metadata).to_dataframe()

        if metric_cols.empty:
            return None, f"No percentile metrics found for table {radar_metrics_table}_seasons"

        # Build the query dynamically
        metric_cols_str = ", ".join(metric_cols['column_name'].tolist())

        query = f"""
            SELECT 
                player_id, 
                season_id,
                {metric_cols_str}
            FROM `{DB_PREFIX}.{radar_metrics_table}_seasons`
            WHERE player_id = {player_id} AND season_id = '{season_id}'
        """

        df = bq_client.query(query).to_dataframe()

        # Convert values to numeric to ensure they are valid for the radar chart
        for col in metric_cols['column_name'].tolist():
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df, None
    except Exception as e:
        return None, str(e)


def create_radar_chart(df, player_name=None):
    if df is None or df.empty:
        return None

    # Get only the percentile metrics columns
    metric_cols = [col for col in df.columns if col.endswith('_percentile')]

    if not metric_cols:
        return None

    # Create display names for the metrics
    categories = [col.replace('_percentile', '').replace('_', ' ').title() for col in metric_cols]

    # Get values, handling potential missing values
    values = [float(df[col].iloc[0]) if not pd.isna(df[col].iloc[0]) else 0.0 for col in metric_cols]

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


# Display similar players section
def display_similar_players(player_id, player_name, position_category):
    st.subheader(f"Similar Players to {player_name}")

    with st.spinner("Loading similar players..."):
        similar_players, error = load_similar_players(player_id, position_category)

    if error:
        st.error(f"Error loading similar players: {error}")
    elif similar_players is None or similar_players.empty:
        st.info(f"No similar players found for {player_name}")
    else:
        # Display similar players in a table
        display_df = similar_players.copy()

        # Format similarity score as percentage
        if 'similarity_score' in display_df.columns:
            display_df['similarity_score'] = (display_df['similarity_score'] * 100).round(1).astype(str) + '%'

        # Rename columns for better display
        display_df = display_df.rename(columns={
            'player_name': 'Similar Player',
            'similarity_score': 'Similarity',
            'birth_date': 'Birth Date'
        })

        # Format the nationality column
        if 'nationality' in display_df.columns:
            try:
                display_df['Nationality'] = display_df['nationality'].apply(
                    lambda x: ', '.join([item['element'] for item in x['list']]) if isinstance(x, dict) and 'list' in x
                    else x
                )
                display_df = display_df.drop(columns=['nationality'])
            except:
                display_df = display_df.rename(columns={'nationality': 'Nationality'})

        # Display the table
        st.dataframe(display_df, use_container_width=True, hide_index=True)


# App title and description
st.title("Football Analytics Dashboard")

# Main content - Player Radar Charts with Position Selection
st.header("Player Radar Charts")

# Load base data - this is needed regardless of user choices
with st.spinner("Loading base data..."):
    positions_df, positions_error = load_positions()
    teams_df, teams_error = load_teams()

if positions_error:
    st.error(f"Error loading positions: {positions_error}")
elif teams_error:
    st.error(f"Error loading teams: {teams_error}")
elif positions_df is not None and teams_df is not None:
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

    # Get the radar metrics table for the selected position category
    selected_radar_table = positions_df[positions_df["category"] == selected_category]["radar_metrics_table"].iloc[0]

    # Create columns for the remaining filters
    col1, col2 = st.columns(2)

    selected_player_ids = None
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
                selected_player_names = st.multiselect(
                    "Select Players (up to 5):",
                    options=player_options,
                    max_selections=5
                )

                # Get all selected player IDs
                selected_player_ids = []
                selected_player_names_to_ids = {}
                for player_name in selected_player_names:
                    player_row = position_players_df[position_players_df["player_name"] == player_name]
                    if not player_row.empty:
                        player_id = int(player_row["player_id"].iloc[0])
                        selected_player_ids.append(player_id)
                        selected_player_names_to_ids[player_name] = player_id

                # Display player details for selected players
                if selected_player_ids:
                    st.write("Selected Players:")
                    for player_name in selected_player_names:
                        player_row = position_players_df[position_players_df["player_name"] == player_name]
                        if not player_row.empty:
                            with st.expander(f"{player_name} Details"):
                                nationality_raw = player_row['nationality'].iloc[0]
                                nationalities = [item['element'] for item in nationality_raw['list']]
                                st.write(f"- Nationality: {', '.join(nationalities)}")
                                st.write(f"- Birth Date: {player_row['birth_date'].iloc[0]}")
            else:
                st.warning(f"No players found for position category: {selected_category}")

    else:  # Season data
        with col1:
            # Load seasons based on the radar metrics table for the selected position
            with st.spinner("Loading seasons..."):
                seasons, season_error = load_seasons(selected_radar_table)

            if season_error:
                st.error(f"Error loading seasons: {season_error}")
            elif seasons is not None:
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
                        selected_player_names = st.multiselect(
                            "Select Players (up to 5):",
                            options=player_options,
                            max_selections=5
                        )

                        # Get the selected player details
                        selected_player_ids = []
                        selected_player_names_to_ids = {}
                        for player_name in selected_player_names:
                            player_row = team_players_df[
                                team_players_df["player_name"] == player_name]
                            if not player_row.empty:
                                player_id = int(player_row["player_id"].iloc[0])
                                selected_player_ids.append(player_id)
                                selected_player_names_to_ids[player_name] = player_id

                        if selected_player_ids:
                            st.write("Selected Players:")
                            for player_name in selected_player_names:
                                player_row = team_players_df[team_players_df["player_name"] == player_name]
                                if not player_row.empty:
                                    with st.expander(f"{player_name} Details"):
                                        nationality_raw = player_row['nationality'].iloc[0]
                                        nationalities = [item['element'] for item in nationality_raw['list']]
                                        st.write(f"- Nationality: {', '.join(nationalities)}")
                                        st.write(f"- Birth Date: {player_row['birth_date'].iloc[0]}")
                    else:
                        st.warning(f"No players found for selected team and position in this season.")
            else:
                st.warning(f"No seasons available for the selected position category.")

    # Load and display player data
    if st.button("Generate Radar Chart"):
        if not selected_player_ids:
            st.warning("Please select at least one player.")
        else:
            with st.spinner("Loading player statistics..."):
                if data_type == "Career":
                    player_data, data_error = load_multiple_player_career_data(selected_player_ids,
                                                                               selected_radar_table)
                else:  # Season
                    player_data, data_error = load_multiple_player_season_data(selected_player_ids, selected_season,
                                                                               selected_radar_table)

            if data_error:
                st.error(f"Error loading player data: {data_error}")
            elif player_data is None or player_data.empty:
                st.warning(f"No {data_type.lower()} data available for the selected players.")
            else:
                # Display the metric data in a table
                display_df = player_data.copy()
                # Keep only relevant columns for display
                if data_type == "Career":
                    display_cols = ['player_name'] + [col for col in display_df.columns if col.endswith('_percentile')]
                else:
                    display_cols = ['player_name', 'season_id'] + [col for col in display_df.columns if
                                                                   col.endswith('_percentile')]

                st.dataframe(display_df[display_cols], use_container_width=True, hide_index=True)

                # Create and display radar chart with multiple players
                metric_cols = [col for col in player_data.columns if col.endswith('_percentile')]
                chart = create_radar_chart_multiple(player_data, metric_cols)
                if chart:
                    st.plotly_chart(chart, use_container_width=True)

                # Add download button for the player data
                st.download_button(
                    label="Download player data as CSV",
                    data=display_df[display_cols].to_csv(index=False).encode('utf-8'),
                    file_name=f"player_comparison_{data_type.lower()}.csv",
                    mime='text/csv',
                )

                # Display similar players section for each selected player
                st.header("Similar Players")

                # Create tabs for each selected player
                if len(selected_player_names) > 0:
                    tabs = st.tabs(selected_player_names)

                    for i, player_name in enumerate(selected_player_names):
                        with tabs[i]:
                            player_id = selected_player_names_to_ids.get(player_name)
                            if player_id:
                                display_similar_players(player_id, player_name, selected_category)

# Footer with app information
st.sidebar.markdown("---")
st.sidebar.info("Football Analytics Dashboard")
st.sidebar.text(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")