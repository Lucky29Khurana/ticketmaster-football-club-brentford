"""
This is a boilerplate pipeline '02_ticket_event_aggregation'
generated using Kedro 0.19.6
"""

from prospect.ticketmaster.ticket_event_aggregation import (
    create_daily_aggregated_sales_df,
    create_daily_aggregated_price_level_sales_df,
    split_home_away_daily_sales_df
)

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def create_aggregated_sales_df(
        formatted_transactions_df: pd.DataFrame,
        formatted_event_df: pd.DataFrame,
        ingestion_params: dict,
) -> [pd.DataFrame, pd.DataFrame]:
    """
    Takes ticketing transactions and aggregates to capture daily or weekly sales & revenue for each match.
    Also, captures cumulative sales and cumulative revenue for each match. The same is then done for each price level.

    Parameters
    ----------
    formatted_transactions_df: pd.DataFrame
        A dataframe with all the ticketing transactions for specific client
    formatted_event_df: pd.DataFrame
        A dataframe with match related information for the TM clients
    ingestion_params: dict
        Dictionary used to identify the client_type

    Returns
    -------
    pd.DataFrame
        A dataframe with aggregated ticket sales & revenue along with cumulative sales & revenue for specific client.

    pd.DataFrame
        A dataframe with aggregated ticket sales & revenue along with cumulative sales & revenue per price level
        for specific client.

    """
    if ingestion_params['client_type'] in ['football_club', 'rugby_club']:
        logger.info("Executing function to calculate aggregated daily sales")

        aggregated_sales_df = create_daily_aggregated_sales_df(
            transactions_df=formatted_transactions_df,
            event_df=formatted_event_df
        )

        logger.info("Executing function to calculate aggregated price level daily sales")

        aggregated_price_level_sales_df = create_daily_aggregated_price_level_sales_df(
            transactions_df=formatted_transactions_df,
            event_df=formatted_event_df
        )

        logger.info("aggregated_sales_df and aggregated_price_level_sales_df successfully created.")

        return aggregated_sales_df, aggregated_price_level_sales_df


def split_aggregated_sales_df(
        aggregated_price_level_sales_df: pd.DataFrame,
        formatted_event_df: pd.DataFrame,
        ingestion_params: dict,
        tea_params: dict,
) -> [pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Converts the sales data into one of the three categories:
    - Home fixtures, home fans
    - Home fixtures, away fans
    - Away fixtures, home fans

    Parameters
    ----------
    aggregated_price_level_sales_df: pd.DataFrame
        A dataframe with aggregated ticket sales & revenue along with cumulative sales & revenue for specific client.
    formatted_event_df: pd.DataFrame
        A dataframe with match related information for the TM clients.
    ingestion_params: dict
        Dictionary used to identify the client_wyscout_name.
    tea_params: dict
        Dictionary used to identify away stands.

    Returns
    -------
    pd.DataFrame
        A dataframe with aggregated ticket sales & revenue for home fixtures and home fans.
    pd.DataFrame
        A dataframe with aggregated ticket sales & revenue for home fixtures and away fans.
    pd.DataFrame
        A dataframe with aggregated ticket sales & revenue for away fixtures and home fans.

    """
    logger.info("Executing function to split aggregated_price_level_sales_df.")

    (
        home_fixtures_home_fans_aggregated_sales_df,
        home_fixtures_away_fans_total_sales_df,
        away_fixtures_aggregated_sales_df
    ) = split_home_away_daily_sales_df(
        daily_price_level_sales_df=aggregated_price_level_sales_df,
        event_df=formatted_event_df,
        wyscout_client_name=ingestion_params['client_wyscout_name'],
        away_stands=tea_params['away_stands']
    )

    logger.info("aggregated_price_level_sales_df successfully split into home_fixtures_home_fans_aggregated_sales_df,"
                "home_fixtures_away_fans_total_sales_df and away_fixtures_aggregated_sales_df.")

    return (
        home_fixtures_home_fans_aggregated_sales_df,
        home_fixtures_away_fans_total_sales_df,
        away_fixtures_aggregated_sales_df,
    )
