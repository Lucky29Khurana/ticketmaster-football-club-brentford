"""
This is a boilerplate pipeline '02_ticket_event_aggregation'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_aggregated_sales_df,
                inputs=[
                    "formatted_transactions_df",
                    "formatted_event_df",
                    "params:ingestion_params",
                ],
                outputs=[
                    "aggregated_sales_df",
                    "aggregated_price_level_sales_df",
                ],
                name="create_aggregated_sales_df_node",
                tags=["football_club_tea_pipeline", "rugby_club_tea_pipeline"]
            ),
            node(
                func=split_aggregated_sales_df,
                inputs=[
                    "formatted_transactions_df",
                    "formatted_event_df",
                    "params:ingestion_params",
                    "params:home_stadium_params"
                ],
                outputs=[
                    "home_fixtures_home_fans_aggregated_sales_df",
                    "home_fixtures_away_fans_total_sales_df",
                    "away_fixtures_aggregated_sales_df",
                ],
                name="split_aggregated_sales_df_node",
                tags=["football_club_tea_pipeline"]
            ),
        ]
    )
