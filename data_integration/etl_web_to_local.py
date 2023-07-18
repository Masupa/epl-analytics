import os
import ssl
from pathlib import Path
from datetime import timedelta
import pandas as pd

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context


@task(name='Extract EPL Data', retries=2, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(days=1))
def extract_epl_data(dataset_url: str) -> pd.DataFrame:
    """Loads data from the web into a pandas DataFrame

    Args:
    -----
    data_urls : str
        URL to a CSV dataset

    Returns:
    --------
    epl_df : pd.DataFrame
    """

    # Load CSV data from URL
    epl_df = pd.read_csv(dataset_url)

    # Fields of interest
    stats_fields = ['Div', 'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG',
                    'FTR', 'HTHG', 'HTAG', 'HTR']

    epl_df = epl_df[stats_fields]

    return epl_df


@task(name='Transform Data', retries=2)
def transform_epl_data(epl_df: pd.DataFrame, epl_season: str) -> pd.DataFrame:
    """Transforms and cleans the EPL datasets

    Args:
    -----
    epl_df : pd.DataFrame
        Pandas DataFrame with EPL data
    epl_season : str
        EPL Season

    Returns:
    --------
    epl_df: pd.DataFrame
    """

    # Convert `Date` col type to datetime
    epl_df['Date'] = pd.to_datetime(epl_df['Date'], dayfirst=True)

    # Add tag for EPL season
    prev_yr = epl_season[:2]
    cur_yr = epl_season[2:]
    epl_season = f'20{prev_yr}-20{cur_yr}'
    epl_df['Season Year'] = epl_season

    return epl_df, epl_season


@task(name='Write to local', retries=2, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(days=1))
def write_local(data_frame: pd.DataFrame, epl_season: str,
                dataset_file_name: str) -> None:
    """Save transformed dataset to local
    file path

    Args:
    -----
    df : pd.DataFrame
        Pandas DataFrame with EPL data
    epl_season : str:
        EPL season
    data_file : str
        EPL file name

    Returns:
        None
    """

    current_path = os.path.dirname(__file__)
    data_path = os.path.join(current_path, 'Data', f'{epl_season}')
    # Check if directory already exits
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    file_path = os.path.join(data_path, f'{dataset_file_name}.parquet')
    data_frame.to_parquet(file_path, compression='gzip')


@task(name='Upload EPL data to GCS', retries=2, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(days=1))
def upload_to_gcs(data_frame: str, epl_season: str,
                  dataset_file_name: str) -> None:
    """Upload EPL data from DataFrame
    to GCS EPL Data Lake

    Args:
    -----
    data_frame : str
        EPL dataset
    epl_season : str
        EPL Season
    dataset_file_name : str
        EPL dataset file name

    Returns:
    --------
        None
    """

    gcp_cloud_storage_bucket_block = GcsBucket.load("epl-gcs-bucket")

    # Upload to GCS
    gcp_cloud_storage_bucket_block.upload_from_dataframe(
        df=data_frame,
        to_path=Path(
            f'epl_match_repository/{epl_season}/{dataset_file_name}.parquet'),
        serialization_format='parquet_gzip'
    )


@flow(name='EPL Data Pipeline')
def etl(url: str, season: str, dataset_file_name: str):
    """ Some Doc String """

    # Load EPL data from web to DataFrame
    epl_df = extract_epl_data(dataset_url=url)
    # Transform data
    cleaned_epl_df, epl_season = transform_epl_data(
        epl_df=epl_df, epl_season=season)
    # Write data to local repository
    write_local(data_frame=cleaned_epl_df, epl_season=epl_season,
                dataset_file_name=dataset_file_name)
    # Load data from DataFrame to GCS
    upload_to_gcs(data_frame=cleaned_epl_df, epl_season=epl_season,
                  dataset_file_name=dataset_file_name)


if __name__ == "__main__":
    seasons = ['1516', '1617', '1718', '1819', '1920', '2021', '2122', '2223']
    DATASET_FILE_NAME = 'epl_match_data'

    for season in seasons:
        epl_data_url = (
            f'https://www.football-data.co.uk/mmz4281/{season}/E0.csv'
        )

        etl(epl_data_url, season, DATASET_FILE_NAME)
