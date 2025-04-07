import streamlit as st
from google.cloud import bigquery
import pandas as pd
import time
import plotly.graph_objects as go

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
def load_players():
    try:
        bq_client = bigquery.Client()
        query = """
            SELECT DISTINCT player_id
            FROM `footballdataengineering.analytics.radar_metrics_centerbacks_career`
            ORDER BY player_id
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


def create_radar_chart(df):
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

    # Convert player_id to string for the trace name
    player_id_str = str(df['player_id'].iloc[0])

    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=player_id_str,
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
        title=f"Radar Chart for Player ID: {player_id_str}"
    )

    return fig


# App title and description
st.title("Football Analytics Dashboard")

# Tab selection
tab1, tab2 = st.tabs(["Positions", "Player Radar Charts"])

with tab1:
    st.header("Football Positions")

    # Load position data
    with st.spinner("Loading position data from BigQuery..."):
        positions_df, error = load_positions()

    if error:
        st.error(f"Error loading positions: {error}")
    elif positions_df is not None:
        # Get unique categories for filtering
        categories = positions_df["category"].unique().tolist()

        # Create filtering by category
        selected_categories = st.multiselect(
            "Filter by Category",
            options=categories,
            default=[]
        )

        # Filter data based on selection
        if selected_categories:
            filtered_df = positions_df[positions_df["category"].isin(selected_categories)]
        else:
            filtered_df = positions_df

        # Display the number of results
        st.write(f"Showing {len(filtered_df)} positions")

        # Display data table
        st.dataframe(
            filtered_df[["position_id", "category"]],
            use_container_width=True,
            hide_index=True
        )

        # Add a download button for the data
        st.download_button(
            label="Download positions as CSV",
            data=filtered_df.to_csv(index=False).encode('utf-8'),
            file_name="football_positions.csv",
            mime='text/csv',
        )

with tab2:
    st.header("Player Radar Charts")

    # Load player and season data
    with st.spinner("Loading player data..."):
        players_df, player_error = load_players()
        seasons, season_error = load_seasons()

    if player_error:
        st.error(f"Error loading players: {player_error}")
    elif season_error:
        st.error(f"Error loading seasons: {season_error}")
    elif players_df is not None and seasons is not None:
        # Data type selection
        data_type = st.radio(
            "Select data type:",
            ["Career", "Season"],
            horizontal=True
        )

        # Player selection
        selected_player = st.selectbox(
            "Select Player ID:",
            options=players_df["player_id"].tolist(),
            index=0
        )

        # Season selection if applicable
        selected_season = None
        if data_type == "Season":
            selected_season = st.selectbox(
                "Select Season:",
                options=seasons,
                index=0
            )

        # Load and display player data
        if st.button("Generate Radar Chart"):
            with st.spinner("Loading player statistics..."):
                if data_type == "Career":
                    player_data, data_error = load_player_career_data(selected_player)
                else:  # Season
                    player_data, data_error = load_player_season_data(selected_player, selected_season)

            if data_error:
                st.error(f"Error loading player data: {data_error}")
            elif player_data is None or player_data.empty:
                st.warning(f"No data available for this player{' and season' if data_type == 'Season' else ''}.")
            else:
                # Display the raw data
                st.subheader(f"Player Statistics (ID: {selected_player})")
                if data_type == "Career":
                    display_df = player_data.drop(columns=["player_id"])
                else:
                    display_df = player_data.drop(columns=["player_id", "season_id"])
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                # Create and display radar chart
                chart = create_radar_chart(player_data)
                if chart:
                    st.plotly_chart(chart, use_container_width=True)

                # Add download button for the player data
                st.download_button(
                    label="Download player data as CSV",
                    data=player_data.to_csv(index=False).encode('utf-8'),
                    file_name=f"player_{selected_player}_{data_type.lower()}.csv",
                    mime='text/csv',
                )

# Footer with app information
st.sidebar.markdown("---")
st.sidebar.info("Football Analytics Dashboard")
st.sidebar.text(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")