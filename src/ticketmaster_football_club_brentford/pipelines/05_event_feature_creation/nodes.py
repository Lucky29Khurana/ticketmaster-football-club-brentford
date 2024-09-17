"""
This is a boilerplate pipeline '05_event_feature_creation'
generated using Kedro 0.19.6
"""

from prospect.ticketmaster.event_feature_creation import (
    load_and_format_athena_venue_locations_data,
    load_and_format_interest_index_data_from_s3,
    load_athena_pre_match_ratings_data,
    format_daily_event_df,
    add_venue_features_to_event_data,
    add_timeslot_and_clash_features_to_event_data,
    add_historical_matches_played_to_event_data,
    add_interest_index_features_to_event_data,
    add_rolling_pre_match_forecast_features_to_event_data,
    add_weather_features_to_event_data,
    combine_event_features
)

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_athena_venue_locations_data(
        ingestion_params: dict,
        efc_params: dict,
) -> pd.DataFrame:
    """
    Loads and formats Wikipedia's venue location data for teams from Athena,

    Parameters
    ----------
    ingestion_params: dict
        Dictionary used to identify relevant s3_staging_dir.

    efc_params: dict
        Dictionary used to venue_locations_entity_sport_id, venue_locations_country_name and get the
        wiki_to_fixtures_data_team_names.

    Returns
    ----------
    pd.DataFrame
        A dataframe with venue locations data for teams loaded from the Athena table.

    """

    logger.info("Executing function to load Wiki Venue locations data from Athena.")

    venue_locations_df = load_and_format_athena_venue_locations_data(
        entity_sport_id=efc_params['venue_locations_entity_sport_id'],
        country=efc_params['venue_locations_country_name'],
        team_name_mapping_dict=efc_params['wiki_to_fixtures_data_team_names'],
        s3_staging_dir=ingestion_params['s3_staging_dir'],
    )

    logger.info("athena_venue_locations_df successfully created.")

    return venue_locations_df


def format_daily_event_data(
        daily_sales_df: pd.DataFrame,
        formatted_event_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Function to merge daily sales statistics for given event with corresponding event in the event dataframe for all
    the fixtures for the given client.

    Parameters
    ----------
    daily_sales_df: pd.DataFrame
        A dataframe with daily sales data for each event in the event data.

    formatted_event_df: pd.DataFrame
        A dataframe with information about all the fixtures for the given client.

    Returns
    ----------
    pd.DataFrame
        A dataframe with daily event features for each event from the start of ticket sales.
    """

    logger.info("Executing function to format daily sales data for each event")

    formatted_daily_event_df = format_daily_event_df(
        daily_sales_df=daily_sales_df,
        formatted_event_df=formatted_event_df
    )

    logger.info("formatted_daily_event_df successfully created.")

    return formatted_daily_event_df


def create_venue_features(
        formatted_event_df: pd.DataFrame,
        venue_locations_df: pd.DataFrame,
        efc_params: dict,
) -> pd.DataFrame:
    """
    Function to merge daily sales statistics for given event with corresponding event in the event dataframe for all
    the fixtures for the given client.

    Parameters
    ----------
    formatted_event_df: pd.DataFrame
        A dataframe with formatted information for each match-day event for the given client.

    venue_locations_df: pd.DataFrame
        A dataframe formatted with wiki data stadium gps coordinates.

    efc_params: dict
        Dictionary used to define the min_local_derby_distance.

    Returns
    ----------
    pd.DataFrame
        A dataframe with home and away venue coordinates merged with event data.
    """

    logger.info("Executing function to create venue features")

    venue_features_df = add_venue_features_to_event_data(
        formatted_event_df=formatted_event_df,
        athena_venue_locations_df=venue_locations_df,
        local_derby_distance=efc_params['min_local_derby_distance']
    )

    logger.info("venue_features_df successfully created.")

    return venue_features_df


def create_timeslot_and_clash_features(
        formatted_event_df: pd.DataFrame,
        fixtures_df: pd.DataFrame,
        efc_params: dict,
) -> pd.DataFrame:
    """
    Identifies clashes of other games withing x hours (specified by the clash_window) of kick off of the client team
    fixture.

    Parameters
    ----------
    formatted_event_df: pd.DataFrame
        A dataframe with formatted information for each match-day event for the given client.

    fixtures_df: pd.DataFrame
        A dataframe with historical fixtures for the given client

    efc_params: dict
        Dictionary used to specify clash_window and fixture_clash_dict.

    Returns
    ----------
    pd.DataFrame
        A dataframe with clash feature added to determine the clash within x hours of kick off of the client team
        fixture.
    """
    logger.info("Executing function to create timeslot and clash features")

    timeslot_and_clash_features_df = add_timeslot_and_clash_features_to_event_data(
        formatted_event_df=formatted_event_df,
        fixtures_df=fixtures_df,
        clash_window=efc_params['fixture_clash_window'],
        fixture_clash_dict=efc_params['fixture_clash_dict']
    )

    logger.info("timeslot_and_clash_features_df successfully crated.")

    return timeslot_and_clash_features_df


def create_historical_matches_played_features(
        formatted_event_df: pd.DataFrame,
        fixtures_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Creates a feature for historical games played between the teams upto the given match_date.

    Parameters
    ----------
    formatted_event_df: pd.DataFrame
        A dataframe with formatted information for each match-day event for the given client.

    fixtures_df: pd.DataFrame
        A dataframe with historical fixtures for the given client

    Returns
    ----------
    pd.DataFrame
        Reduced event fixtures dataframe with additional column added for "historical games played" between the team.
    """
    logger.info("Executing function to compute historical matches played between teams")

    historical_matches_played_features_df = add_historical_matches_played_to_event_data(
        formatted_event_df=formatted_event_df,
        fixtures_df=fixtures_df,
    )

    logger.info("historical_matches_played_df successfully created.")

    return historical_matches_played_features_df


def create_interest_index_features(
        formatted_daily_event_df: pd.DataFrame,
        ingestion_params: dict,
        efc_params: dict,
) -> pd.DataFrame:
    """
   Loads and formats interest index data, creating features for the both home and away teams.

   Parameters
   ----------
   formatted_daily_event_df: pd.DataFrame
       A dataframe with formatted information for each match-day event for the given client.

   ingestion_params: dict
       Dictionary used to specify the s3_bucket_name and s3_staging_dir.

   efc_params: dict
       Dictionary used to specify the interest_index_entity_sport_name, interest_index_country_name and
       interest_index_time_period.

   Returns
   ----------
   pd.DataFrame
       A dataframe dataframe with interest index added for home and away teams.
   """
    logger.info("Executing function to load and format Interest Index data from s3.")

    interest_index_df = load_and_format_interest_index_data_from_s3(
        daily_event_df=formatted_daily_event_df,
        interest_index_entity_sport_name=efc_params['interest_index_entity_sport_name'],
        interest_index_country_name=efc_params['interest_index_country_name'],
        interest_index_time_period=efc_params['interest_index_time_period'],
        bucket_name=ingestion_params['s3_bucket_name'],
        s3_staging_dir=ingestion_params['s3_staging_dir'],
    )

    logger.info("Interest Index data successfully loaded.")

    logger.info("Executing function to create Interest Index features.")

    interest_index_features_df = add_interest_index_features_to_event_data(
        daily_event_df=formatted_daily_event_df,
        interest_index_df=interest_index_df,
    )

    logger.info("interest_index_features_df successfully created.")

    return interest_index_features_df


def create_rolling_pre_match_forecast_features(
        formatted_daily_event_df: pd.DataFrame,
        ingestion_params: dict,
        efc_params: dict,
) -> [pd.DataFrame, pd.DataFrame]:
    """
    Converts blended forecast dataframe into rolling daily home and away value for each team to provide a team rating
    for a team on any day for which tickets might be sold.

    Parameters
    ----------
    formatted_daily_event_df: pd.DataFrame
        A dataframe with formatted information for each match-day event for the given client.

    ingestion_params: dict
        Dictionary used to specify the s3_staging_dir.

    efc_params: dict
        Dictionary used to specify the competition_area_name and rolling_window_start_date.

    Returns
    ----------
    pd.DataFrame
        A dataframe with rolling squad metrics from rolling window start date to the latest available data.
    """

    logger.info("Executing function to load Pre Match Ratings data from Athena.")

    if ingestion_params['client_type'] == 'rugby_club':
        pass

    elif ingestion_params['client_type'] == 'football_club':

        pre_match_ratings_df = load_athena_pre_match_ratings_data(
            competition_area_name=efc_params['competition_area_name'],
            s3_staging_dir=ingestion_params['s3_staging_dir'],
        )
        logger.info("Pre Match Ratings data successfully loaded.")

        logger.info("Executing function to create rolling pre Match forecast features.")

        rolling_pre_match_forecast_features_df, all_teams_rolling_pre_match_ratings_df = (
            add_rolling_pre_match_forecast_features_to_event_data(
                daily_event_df=formatted_daily_event_df,
                pre_match_ratings_df=pre_match_ratings_df,
                rolling_window_start_date=efc_params['rolling_window_start_date']
            )
        )

        logger.info("rolling_pre_match_forecast_features_df and all_teams_rolling_pre_match_ratings_df successfully"
                    "created.")

        return rolling_pre_match_forecast_features_df, all_teams_rolling_pre_match_ratings_df

    else:
        raise Exception('Client type is invalid.')


def create_weather_features(
        formatted_event_df: pd.DataFrame,
        venue_locations_df: pd.DataFrame,
        efc_params: dict
) -> pd.DataFrame:
    """
    Merges weather data onto match day fixture data.

    Parameters
    ----------
    formatted_event_df: pd.DataFrame
        A dataframe with formatted information for each match-day event for the given client.

    venue_locations_df: pd.DataFrame
        A dataframe formatted with wiki data stadium gps coordinates.

    efc_params: dict
        Dictionary used to specify the weather_ko_window.

    Returns
    ----------
    pd.DataFrame
        A dataframe with rolling squad metrics from rolling window start date to the latest available data.
    """
    logger.info("Executing function to create weather features")

    weather_features_df = add_weather_features_to_event_data(
        formatted_event_df=formatted_event_df,
        athena_venue_locations_df=venue_locations_df,
        weather_ko_window=efc_params['weather_ko_window']
    )

    logger.info("weather_features_df successfully created.")

    return weather_features_df


def create_daily_event_features_df(
        formatted_daily_event_df: pd.DataFrame,
        venue_features_df: pd.DataFrame,
        timeslot_and_clash_features_df: pd.DataFrame,
        historical_matches_played_df: pd.DataFrame,
        interest_index_features_df: pd.DataFrame,
        rolling_pre_match_forecast_features_df: pd.DataFrame,
        weather_features_df: pd.DataFrame,
) -> pd.DataFrame:
    """
   Combines all the event specific features with fixtures information.

   Parameters
   ----------
   formatted_daily_event_df: pd.DataFrame
       A dataframe with formatted information for each match-day event for the given client.

   venue_features_df: pd.DataFrame
       A dataframe with team venue locations data.

   timeslot_and_clash_features_df: pd.DataFrame
       A dataframe with timeslot and clash features data.

   historical_matches_played_df: pd.DataFrame
       A dataframe with historical matches played data.

   interest_index_features_df: pd.DataFrame
       A dataframe with interest index features data.

   rolling_pre_match_forecast_features_df: pd.DataFrame
       A dataframe with formatted pre match forecast features data.

   weather_features_df: pd.DataFrame
       A dataframe with matchday weather data.

   Returns
   ----------
   pd.DataFrame
       A dataframe with all the event related features computed in the pipeline combined into one single dataframe
   """
    logger.info("Executing function to combined all event features.")

    daily_event_features_df = combine_event_features(
        formatted_daily_event_df=formatted_daily_event_df,
        venue_features_df=venue_features_df,
        timeslot_and_clash_features_df=timeslot_and_clash_features_df,
        historical_matches_played_df=historical_matches_played_df,
        interest_index_features_df=interest_index_features_df,
        pre_match_forecast_features_df=rolling_pre_match_forecast_features_df,
        weather_features_df=weather_features_df,
    )

    logger.info("daily_event_features_df successfully created.")

    return daily_event_features_df
