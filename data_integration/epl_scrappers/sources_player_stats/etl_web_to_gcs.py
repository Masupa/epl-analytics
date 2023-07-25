"""Module implementing ETL Pipeline"""

import os
import ssl
import time
from datetime import timedelta
from datetime import datetime
import pandas as pd

# Import Prefect
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, \
    ElementClickInterceptedException


# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context


@task(name='Extract Player Stats', retries=2, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(days=1))
def extract_player_data(web_url: str, epl_season: str) -> pd.DataFrame:
    """Extract player data from the web

    Args:
    -----
    web_url : str
        web_url to extract data from
    epl_season : str
        EPL season

    Returns:
        Pandas DataFrame with EPL data
    """

    # Lists to keep player information
    players_details = {
        'Player': [],
        'Mins': [],
        'Goals': [],
        'Assists': [],
        'Yel': [],
        'Red': [],
        'SpG': [],
        'PS%': [],
        'AerialsWon': [],
        'MotM': []
    }

    driver = webdriver.Chrome()

    try:
        # Open web browser and wait for it to load
        driver.get(web_url)

        # Wait for the initial element to be present before proceeding
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, '//span[@class="iconize iconize-icon-left"]'))
        )

        def find_stat(xpath, stat_name):
            """Find elements related to a stat
            and appends it to the stats details

            Args:
            -----
            xpath : str:
                XPATH with elements
            stat_name : str:
                Stat name for players

            Return:
            -------
                None
            """

            elements = driver.find_elements(By.XPATH, xpath)
            for element in elements:
                if stat_name == 'Player' and element.text == 'England':
                    pass
                else:
                    players_details[stat_name].append(element.text)

        data_page = 1

        while data_page <= 30:
            try:
                # Find elements with player stats on the current page
                find_stat(
                    '//span[@class="iconize iconize-icon-left"]', 'Player')
                find_stat('//td[@class="minsPlayed   "]', 'Mins')
                find_stat('//td[@class="goal   "]', 'Goals')
                find_stat('//td[@class="assistTotal   "]', 'Assists')
                find_stat('//td[@class="yellowCard   "]', 'Yel')
                find_stat('//td[@class="redCard   "]', 'Red')
                find_stat('//td[@class="shotsPerGame   "]', 'SpG')
                find_stat('//td[@class="passSuccess   "]', 'PS%')
                find_stat('//td[@class="aerialWonPerGame   "]', 'AerialsWon')
                find_stat('//td[@class="manOfTheMatch   "]', 'MotM')

            except TimeoutException:
                print(
                    f'TimeoutException occurred on page'
                    f'{data_page}. Retrying...'
                    )
                continue

            # Break loop at this point
            if data_page == 30:
                break

            next_page_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "next"))
            )

            try:
                # Scroll to the next page button to ensure it's clickable
                driver.execute_script(
                    "arguments[0].scrollIntoView();", next_page_btn)
                time.sleep(1)  # A small pause to ensure the page loads

                # Click the "Next" button
                next_page_btn.click()

                # Wait for the new page to load before continuing
                WebDriverWait(driver, 10).until(
                    EC.staleness_of(next_page_btn)
                )

                data_page += 1

            except ElementClickInterceptedException:
                print(
                    f'ElementClickInterceptedException occurred '
                    f'on page {data_page}. Retrying...'
                    )
                continue

        data_len = len(players_details['Player'])

        # Update EPL season
        players_details['Season'] = [epl_season] * data_len
        
        # Take note of time data is ingested
        players_details['CreatedAt'] = [datetime.utcnow()] * data_len
        players_details['UpdatedAt'] = [datetime.utcnow()] * data_len

        return pd.DataFrame(players_details)

    finally:
        # Close web browser
        driver.close()


@task(name='Transform data', retries=2)
def transform_player_stats_data(player_stats_df: pd.DataFrame) -> pd.DataFrame:
    """Perform transformations on the players
    stats dataframe

    Args:
    -----
    player_stats_df : pd.DataFrame
        Pands DataFrame containing player stats

    Returns:
        player_stats_df : Transformed player stats DataFrame
    """

    return player_stats_df


@task(name='Load data to local', retries=2, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=1))
def load_to_local(player_stats_df: pd.DataFrame, epl_season: str,
                  dataset_file_name: str):
    """Save transformed dataset to local
    file path

    Args:
    -----
    player_stats_df : pd.DataFrame
        Pandas DataFrame with EPL data
    epl_season : str:
        EPL season
    data_file : str
        EPL file name

    Returns:
        None
    """

    current_path = os.path.dirname(__file__)
    data_path = os.path.join(
        current_path,
        'Data',
        f'{epl_season}')
    # Check if directory already exits
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    file_path = os.path.join(data_path, f'{dataset_file_name}.parquet')
    player_stats_df.to_parquet(file_path, compression='gzip')


@task(name='Load data to GCS', retries=2, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=1))
def load_to_gcs(player_stats_df: pd.DataFrame, epl_season: str,
                data_file_name: str):
    """Load transformed data into Google
    Cloud Storage Bucket
    Args:
    -----
    player_stats_df : pd.DataFrame
        EPL player stats dataframe
    epl_season : str
        EPL Season
    dataset_file_name : str
        EPL dataset file name

    Returns:
    --------
        None
    """

    gcp_cloud_storage_bucket_block = GcsBucket.load("epl-gcs-bucket")

    gcp_cloud_storage_bucket_block.upload_from_dataframe(
        df=player_stats_df,
        to_path=f'epl_player_stats_repository/{epl_season}/{data_file_name}.parquet',
        serialization_format='parquet_gzip'
    )


@flow(name='EPL Player Stats Pipeline')
def etl(web_url: str, data_file_name: str, season: str) -> None:
    """Extract EPL player stats from the web,
    transform it, and load it into Cloud Storage

    Args:
    -----
    web_urls : str
        web_url to extract data from
    data_file_name : str
        data file name
    season : str
        league season

    Returns:
    --------
        None
    """

    # Extract player stats data
    player_stats_df = extract_player_data(
        web_url=web_url, epl_season=season)

    # Transform data
    player_stats_df = transform_player_stats_data(player_stats_df)

    # Load to local
    # load_to_local(player_stats_df, season, data_file_name)

    # Load to GCS
    load_to_gcs(player_stats_df, season, data_file_name)


if __name__ == '__main__':
    season_years = ['2012-2013', '2013-2014', '2014-2015',
                    '2015-2016', '2016-2017', '2017-2018',
                    '2018-2019', '2019-2020', '2020-2021',
                    '2021-2022', '2022-2023']

    DATASET_FILE_NAME = 'epl_player_stats_data'

    for season_year in season_years:
        # Build URL
        url = (
            f'https://www.whoscored.com/Regions/252/Tournaments/2/Seasons/8228/'
            f'Stages/18685/PlayerStatistics/England-Premier-League-{season_year}'
        )

        etl(web_url=url, data_file_name=DATASET_FILE_NAME, season=season_year)
