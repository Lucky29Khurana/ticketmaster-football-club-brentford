from typing import Any, Dict

import numpy as np

from kedro.io import AbstractDataset
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pandas as pd
import boto3
import time

from kedro_datasets.pandas import (

    ParquetDataset,
)


class PyAthenaSQLDataset(AbstractDataset[pd.DataFrame, pd.DataFrame]):
    """``CustomAthenaPandasDataset`` loads / save data from a given
    sql query as a pandas dataframe

    """

    def __init__(self, sql_query: str, s3_staging_dir: str, region_name: str):
        """Creates a new instance of CustomAthenaPandasDataset to load / save
        data for given sql query.

        Args:
            sql_query: sql query for athena table
            s3_staging_dir: s3 staging dict for env on aws
            region_name: name of the region input.
        """
        self.sql_query = sql_query
        self.s3_staging_dir = s3_staging_dir
        self.region_name = region_name

    def _load(self) -> pd.DataFrame:
        """
        """
        with connect(
            s3_staging_dir=self.s3_staging_dir,
            output_location=self.s3_staging_dir,
            region_name=self.region_name,
            cursor_class=PandasCursor,
            profile_name='default',
            schema_name='football',
            work_group='primary',
        ).cursor() as cursor:

            pandas_df = cursor.execute(self.sql_query).as_pandas()

        return pandas_df

    def _save(self, data: pd.DataFrame) -> None:
        """Saves data to the specified filepath."""

        data_set = ParquetDataset(filepath="test.parquet")
        return data_set.save(data)

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(s3_query=self.sql_query, save_location=self.s3_staging_dir)


class CustomAthenaPandasDataset(AbstractDataset[pd.DataFrame, pd.DataFrame]):
    def __init__(
            self, sql_query: str, region_name: str, s3_staging_dir, database):
        self.sql_query = sql_query
        self.region_name = region_name
        self.s3_staging_dir = s3_staging_dir
        self.database = database

    def _load(self) -> pd.DataFrame:
        client = boto3.client('athena', region_name=self.region_name)
        response = client.start_query_execution(
            QueryString=self.sql_query,
            QueryExecutionContext={
                'Database': self.database  # Specify your Athena database name
            },
            ResultConfiguration={
                'OutputLocation': self.s3_staging_dir
            }
        )
        query_execution_id = response['QueryExecutionId']

        # Wait for query execution to finish
        while True:
            query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
            state = query_status['QueryExecution']['Status']['State']
            if state in ['QUEUED', 'RUNNING']:
                time.sleep(5)  # Wait for 5 seconds before checking again
            elif state=='SUCCEEDED':
                # Query execution is successful, continue with the rest of your code
                break
            else:
                # Query execution failed or cancelled, handle the error accordingly
                raise ValueError(f"Query execution failed with state: {state}")

        # Get query results
        query_results = client.get_query_results(QueryExecutionId=query_execution_id)
        column_names = [col['Label'] for col in query_results[
            'ResultSet']['ResultSetMetadata']['ColumnInfo']]

        # load the rows data..

        combined_row_list = []

        for response_data in query_results['ResultSet']['Rows'][1:]:
            row_list = []
            for row_item in response_data['Data']:
                try:
                    row_list.append(row_item['VarCharValue'])
                except:
                    row_list.append('')
            combined_row_list.append(row_list)

        data = pd.DataFrame(combined_row_list, columns=column_names)

        return data

    def _save(self, data: pd.DataFrame) -> None:
        """Not implemented for loading only"""

    def _describe(self) -> Dict[str, Any]:
        return dict(s3_query=self.sql_query, save_location=self.s3_staging_dir)
