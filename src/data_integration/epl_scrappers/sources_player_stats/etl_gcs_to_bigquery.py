import os
import argparse
import numpy as np
import pandas as pd
from datetime import timedelta
from dotenv import load_dotenv

from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


# Take env variables from .env
load_dotenv()

# Catch env variables
project_id = os.getenv('project_id')
player_stats_dataset = os.getenv('epl_analytics_dwh')
player_stats_table = os.getenv('player_stats_table')


@task(name='extract from GCS', retries=2,
      cache_expiration=timedelta(microseconds=1),
      cache_key_fn=task_input_hash)
def extract_from_gcs_to_local(gcs_file_path: str) -> str:
    """Download objects within a folder
    from the bucket

    Args:
    -----
    gcs_file_path : str
        Folder path in GCS

    Returns:
        local file path to data
    """

    gcp_cloud_storage_bucket = GcsBucket.load("epl-gcs-bucket")

    gcp_cloud_storage_bucket.get_directory(
        from_path=gcs_file_path,
        local_path='./'
    )

    return f'./{gcs_file_path}'


@task(name='Transform to DataFrame', retries=2)
def transform_to_df(data_file_path: str):
    """Load data into Pandas DataFrame

    Args:
    -----
    data_file_path : str
        file path to data

    Returns:
    --------
        df : pd.DataFrame
    """

    df = pd.read_parquet(data_file_path)

    # Filter values with NaN where there is no record
    df['Goals'] = df['Goals'].replace('-', np.nan)
    df['Assists'] = df['Assists'].replace('-', np.nan)
    df['Yel'] = df['Yel'].replace('-', np.nan)
    df['Red'] = df['Red'].replace('-', np.nan)
    df['SpG'] = df['SpG'].replace('-', np.nan)
    df['PS_'] = df['PS%'].replace('-', np.nan)
    df['AerialsWon'] = df['AerialsWon'].replace('-', np.nan)
    df['MotM'] = df['MotM'].replace('-', np.nan)

    # Remove `PS%` field
    df.drop(columns='PS%', inplace=True)

    return df


@task(name='load to BigQuery', retries=2)
def load_to_bq(df: pd.DataFrame) -> None:
    """Load DataFrame to BigQuery

    Args:
    df : pd.DataFrame
        A pandas DataFrame with EPL stats data

    Returns:
        None
    """

    gcp_credentials = GcpCredentials.load("epl-analytics-gcs-credentials")

    df.to_gbq(
        project_id=project_id,
        destination_table=player_stats_table,
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=300,
        if_exists="append"
    )


@flow(name='GCS to BigQuery')
def etl(gcs_file_path: str):
    """Flow responsible for call func
    to get data from GCS to BigQuery

    Args:
    -----
    gcs_folder_path : str
        Folder path in GCS

    Returns:
    --------
        None
    """

    # Load data to BigQuery
    file_path = extract_from_gcs_to_local(gcs_file_path)

    # Load data into Pandas DataFrame
    df = transform_to_df(file_path)

    # Load data to BigQuery
    load_to_bq(df)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description='Pass Season Year'
    )
    arg_parser.add_argument('--season_year', type=str, required=True)

    season = arg_parser.parse_args().season_year

    current_local_path = os.path.dirname(__file__)

    GCS_PATH = (
        f'epl_player_stats_repository/{season}/'
        f'epl_player_stats_data.gz.parquet'
    )

    # Extract data from GCS to BigQuery
    etl(GCS_PATH)
