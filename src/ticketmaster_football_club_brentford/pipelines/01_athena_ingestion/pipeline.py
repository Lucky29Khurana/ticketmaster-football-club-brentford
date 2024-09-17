"""
This is a boilerplate pipeline '01_athena_ingestion'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_and_format_athena_event_data,
                inputs="params:ingestion_params",
                outputs=[
                    "formatted_event_df",
                    "fixtures_df",
                ],
                name="load_and_format_athena_event_data_node",
                tags=[
                    'football_club_ai_pipeline',
                    'rugby_club_ai_pipeline',
                    'rugby_international_ai_pipeline'
                ],
            ),

            node(
                func=load_and_format_transaction_data,
                inputs=[
                    "formatted_event_df",
                    "params:ingestion_params"
                ],
                outputs="formatted_transactions_df",
                name="load_and_format_transaction_data_node",
                tags=[
                    'football_club_ai_pipeline',
                    'rugby_club_ai_pipeline',
                    'rugby_international_ai_pipeline'
                ]
            ),

            node(
                func=load_member_data,
                inputs="params:ingestion_params",
                outputs="members_df",
                name="load_and_format_member_data_node",
                tags=[
                    'football_club_ai_pipeline',
                    'rugby_club_ai_pipeline',
                    'rugby_international_ai_pipeline'
                ]
            ),

            node(
                func=load_and_format_season_card_data,
                inputs="params:ingestion_params",
                outputs="formatted_season_card_df",
                name="load_and_format_season_card_data_node",
                tags=[
                    'football_club_ai_pipeline',
                    'rugby_club_ai_pipeline',
                    'rugby_international_ai_pipeline'
                ]
            ),

            node(
                func=load_and_format_ticketscan_data,
                inputs=[
                    "formatted_event_df",
                    "params:ingestion_params"
                ],
                outputs="formatted_ticketscan_df",
                name="load_and_format_ticketscan_data_node",
                tags=[
                    'football_club_ai_pipeline',
                    'rugby_club_ai_pipeline',
                    'rugby_international_ai_pipeline'
                ]
            )
        ]
    )
