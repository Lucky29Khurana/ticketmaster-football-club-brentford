"""
This is a boilerplate pipeline '01_athena_ingestion'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import (
    load_and_format_athena_event_data,
    load_and_format_transaction_data,
    load_and_format_member_data,
    load_and_format_season_card_data,
    load_and_format_ticketscan_data
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_and_format_athena_event_data,
                inputs="params:ingestion_params",
                outputs="formatted_event_df",
                name="formatted_event_df_node"
            ),

            node(
                func=load_and_format_transaction_data,
                inputs=[
                    "formatted_event_df",
                    "params:ingestion_params"
                ],
                outputs="all_transactions_df",
                name="load_athena_member_data_node"
            ),

            node(
                func=load_and_format_member_data,
                inputs="params:ingestion_params",
                outputs="all_members_df",
                name="load_and_format_member_data_node"
            ),

            node(
                func=load_and_format_season_card_data,
                inputs="params:ingestion_params",
                outputs="formatted_season_card_df",
                name="load_and_format_member_data_node"
            ),

            node(
                func=load_and_format_ticketscan_data,
                inputs=[
                    "formatted_event_df",
                    "params:ingestion_params"
                ],
                outputs="formatted_ticketscan_df",
                name="load_and_format_ticketscan_data_node"
            )
        ]
    )
