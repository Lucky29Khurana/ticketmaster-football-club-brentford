"""
This is a boilerplate pipeline '01_athena_ingestion'
generated using Kedro 0.19.6
"""
from prospect.ticketmaster.athena_ingestion import *
import logging

logger = logging.getLogger(__name__)


# NODES

def load_and_format_athena_event_data(
        ingestion_params: dict,
) -> [pd.DataFrame, pd.DataFrame]:
    """
    Loads and formats match event data for the relevant client in athena. Matches are matched using Wyscout data for
    football clients and Oval data for rugby clients.

    Parameters
    ----------
    ingestion_params: dict
        Dictionary used to identify relevant parameters and s3_staging_dir.

    Returns
    -------
    pd.DataFrame
        A dataframe containing event data from the Athena Table for the specific client's event

    pd.DataFrame
        A dataframe containing fixtures data from Snowflake for specific ticketmaster client

    Raises
    ------
    Exception
        If 'client_type' from 'ingestion_params' is a string of the type 'rugby_club', 'football_club',
        and 'rugby_international'.
    """
    logger.info("Executing function to load and format event data.")

    if ingestion_params['client_type'] == 'rugby_club':
        pass

    elif ingestion_params['client_type'] == 'football_club':
        event_df, fixtures_df = load_and_format_athena_football_event_data(
            ticketmaster_client_name=ingestion_params['client_athena_name'],
            s3_staging_dir=ingestion_params['s3_staging_dir'],
            wyscout_client_team_id=int(ingestion_params['client_wyscout_id']),
            wyscout_client_team_name=ingestion_params['client_wyscout_name'],
            additional_events_filter_keywords=ingestion_params['additional_events_filter_keywords'],
            snowflake_region=ingestion_params['snowflake_region'],
            snowflake_secret_key=ingestion_params['snowflake_secret_key']
        )

        return event_df, fixtures_df

    elif ingestion_params['client_type'] == 'rugby_international':
        pass
    else:
        raise Exception('Client type is invalid.')

    logger.info('formatted_event_df and fixtures_df successfully created.')


def load_and_format_transaction_data(
        formatted_event_df: pd.DataFrame,
        ingestion_params: dict,
) -> pd.DataFrame:
    """
    Loads and formats transaction data for the given client. Transactions are filtered for match-days (as specified in
    formatted_event_df) and season_ticket_ids.

    Parameters
    ----------
    formatted_event_df: pd.DataFrame
        A dataframe with formatted information for each match-day event for the given client.
    ingestion_params: dict
        Dictionary used to identify season_ticket_event_ids and the client_athena_name and s3_staging_dir..

    Returns
    -------
    pd.DataFrame
        A dataframe with formatted transaction information for each match-day event / season ticket purchase for the
        given client.

        """
    logger.info("Executing function to load transaction data.")

    logger.info("Extracting relevant event ids for which we need to extract Transactions Data")
    event_ids = ingestion_params['season_ticket_event_ids'].values() + formatted_event_df['event_data_id'].tolist()

    transactions_df = load_and_format_athena_transaction_data(
        ticketmaster_client_name=ingestion_params['client_athena_name'],
        s3_staging_dir=ingestion_params['s3_staging_dir'],
        event_ids=event_ids,
    )

    logger.info("formatted_transactions_df successfully created.")

    return transactions_df


def load_member_data(
        ingestion_params: dict,
) -> pd.DataFrame:
    logger.info("Executing function to load TM Members Data from Athena")

    members_df = load_athena_member_data(
        ticketmaster_client_name=ingestion_params['client_athena_name'],
        s3_staging_dir=ingestion_params['s3_staging_dir'],
    )

    logger.info("Members Data loaded successfully from Athena")

    return members_df


def load_and_format_season_card_data(
        ingestion_params: dict,
) -> pd.DataFrame:
    logger.info("Executing function to load and format TM Season Card Data from Athena")

    season_card_df = load_and_format_season_card_data(
        ticketmaster_client_name=ingestion_params['client_athena_name'],
        s3_staging_dir=ingestion_params['s3_staging_dir'],
    )

    # renaming values of event_name column to relevant values
    season_card_df['event_name'] = season_card_df['event_name'].replace({
        'Season 2023\\/24': 'Season 2023/24',
        'Season 2024\\/25': 'Season 2024/25',
    })

    # dropping rows with irrelevant events
    season_card_df = season_card_df[~(season_card_df['event_name'] == 'Test Season 23\/24')]

    logging.info("Formatted Season Card Data loaded successfully from Athena")

    return season_card_df


def load_and_format_ticketscan_data(
        formatted_event_df: pd.DataFrame,
        ingestion_params: dict,
) -> pd.DataFrame:
    logger.info("Executing function to load and format TM Ticket Scan Data from Athena")

    event_ids = formatted_event_df[formatted_event_df['competition_name'].isin(ingestion_params['competition_name'])][
        'event_data_id'].unique().tolist()

    ticketscan_df = load_and_format_athena_ticketscan_data(
        ticketmaster_client_name=ingestion_params['client_athena_name'],
        s3_staging_dir=ingestion_params['s3_staging_dir'],
        league_matches_event_ids=event_ids,
    )

    logger.info("Formatted Ticket Scan Data loaded successfully from Athena")

    return ticketscan_df
