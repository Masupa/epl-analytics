import os
import ssl
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash


# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context


@task(name='Extract EPL Data', retries=2, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=1))
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
def transform_epl_data(epl_df: pd.DataFrame) -> pd.DataFrame:
    """Transforms and cleans the EPL datasets

    Args:
    -----
    epl_df : pd.DataFrame
        Pandas DataFrame with EPL data

    Returns:
    --------
    epl_df: pd.DataFrame
    """

    # Convert `Date` col type to datetime
    epl_df['Date'] = pd.to_datetime(epl_df['Date'], dayfirst=True)

    return epl_df


@task(name='Write to local', retries=2)
def write_local(df: pd.DataFrame, season: str) -> None:
    """Save transformed dataset to local
    file path

    Args:
    -----
    df : pd.DataFrame
        Pandas DataFrame with EPL data

    Returns:
    None
    """

    current_path = os.path.dirname(__file__)
    data_path = os.path.join(current_path, 'Data', f'{season}')
    # Check if directory already exits
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    file_path = os.path.join(data_path, f'premier_league_data_{season}.csv')
    df.to_csv(file_path)


@flow(name='EPL Data Pipeline')
def etl(url: str, season: str):
    """ Some Doc String """

    epl_df = extract_epl_data(dataset_url=url)
    cleaned_epl_df = transform_epl_data(epl_df=epl_df)
    write_local(df=cleaned_epl_df, season=season)


if __name__ == "__main__":
    seasons = ['1516', '1617', '1718', '1819', '1920', '2021', '2122', '2223']

    for season in seasons:
        epl_data_url = f'https://www.football-data.co.uk/mmz4281/{season}/E0.csv'

        etl(url=epl_data_url, season=season)
