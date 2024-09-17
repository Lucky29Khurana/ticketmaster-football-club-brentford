"""
This is a boilerplate pipeline '05_event_feature_creation'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_athena_venue_locations_data,
                inputs=[
                    "params:ingestion_params",
                    "params:efc_params",
                ],
                outputs="athena_venue_locations_df",
                name="load_athena_venue_locations_data_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=format_daily_event_data,
                inputs=[
                    "daily_sales_df",
                    "formatted_event_df",
                ],
                outputs="formatted_daily_event_df",
                name="format_daily_event_data_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=create_venue_features,
                inputs=[
                    "formatted_event_df",
                    "athena_venue_locations_df",
                    "params:efc_params",
                ],
                outputs="venue_features_df",
                name="create_venue_features_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=create_timeslot_and_clash_features,
                inputs=[
                    "formatted_event_df",
                    "fixtures_df",
                    "params:efc_params",
                ],
                outputs="timeslot_and_clash_features_df",
                name="create_timeslot_and_clash_features_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=create_historical_matches_played_features,
                inputs=[
                    "formatted_event_df",
                    "fixtures_df",
                ],
                outputs="historical_matches_played_df",
                name="create_historical_matches_played_features_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=create_interest_index_features,
                inputs=[
                    "formatted_daily_event_df",
                    "params:ingestion_params",
                    "params:efc_params",
                ],
                outputs="interest_index_features_df",
                name="create_interest_index_features_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=create_rolling_pre_match_forecast_features,
                inputs=[
                    "formatted_daily_event_df",
                    "params:ingestion_params",
                    "params:efc_params",
                ],
                outputs=[
                    "rolling_pre_match_forecast_features_df",
                    "all_teams_rolling_pre_match_ratings_df",
                ],
                name="create_rolling_pre_match_forecast_features_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=create_weather_features,
                inputs=[
                    "formatted_event_df",
                    "athena_venue_locations_df",
                    "params:efc_params",
                ],
                outputs="weather_features_df",
                name="create_weather_features_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            ),
            node(
                func=create_daily_event_features_df,
                inputs=[
                    "formatted_daily_event_df",
                    "venue_features_df",
                    "timeslot_and_clash_features_df",
                    "historical_matches_played_df",
                    "interest_index_features_df",
                    "rolling_pre_match_forecast_features_df",
                    "weather_features_df",
                ],
                outputs="daily_event_features_df",
                name="create_daily_event_features_df_node",
                tags=["football_club_efc_pipeline", "rugby_club_efc_pipeline"],
            )
        ]
    )
