"""
This is a boilerplate pipeline '01_athena_ingestion'
generated using Kedro 0.19.6
"""
from prospect.ticketmaster.athena_ingestion import *
import logging
logger = logging.getLogger(__name__)


# NODES

def load_and_format_athena_event_data(
        params: dict,
) -> pd.DataFrame:

    if params['client_type'] == 'rugby_club':
        # df =  load_and_format_oval_rugby_event_data(
        #
        # )
        pass
    elif params['client_type'] == 'football_club':
        df = load_and_format_athena_football_event_data(
            ticketmaster_client_name=params['client_athena_name'],
            s3_staging_dir=params['s3_staging_dir'],
            wyscout_client_team_id=params['client_wyscout_id'],
            wyscout_client_team_name=params['client_wyscout_name'],
            additional_events_filter_keywords=params['additional_events_filter_keywords'],
            snowflake_region="",     # TODO: add snowflake credentials
            snowflake_secret_key="",
        )
    elif params['client_type'] == 'rugby_international':
        # df = load_and_format_oval_rugby_event_data(
        #
        # )
        pass
    else:
        raise Exception('Client type is invalid.')

    logger.info('formatted_event_df successfully created.')

    return df


def load_and_format_transaction_data(
        formatted_event_df: pd.DataFrame,
        params: dict,
) -> pd.DataFrame:

    event_ids = params['season_ticket_event_ids'].values() + formatted_event_df['event_data_id'].tolist()

    df = load_and_format_athena_transaction_data(
        ticketmaster_client_name=params['client_athena_name'],
        event_ids=event_ids
    )

    logger.info('all_transactions_df successfully created.')

    return df


def load_and_format_member_data(
        params: dict,
) -> pd.DataFrame:

    df = load_athena_member_data(
        ticketmaster_client_name=params['client_athena_name']
    )

    logger.info('all_members_df successfully created.')

    return df


def load_and_format_season_card_data(
        params: dict,
) -> pd.DataFrame:

    df = load_and_format_season_card_data(
        ticketmaster_client_name=params['client_athena_name']
    )

    # TODO: make sure that event_name column is proper

    logging.info('formatted_season_card_df successfully created.')

    return df


def load_and_format_ticketscan_data(
        formatted_event_df: pd.DataFrame,
        params: dict,
) -> pd.DataFrame:

    event_ids = formatted_event_df['event_data_id'].tolist()

    df = load_and_format_athena_ticketscan_data(
        ticketmaster_client_name=params['client_athena_name'],
        league_matches_event_ids=event_ids
    )

    logger.info('formatted_ticketscan_df successfully created.')

    return df
