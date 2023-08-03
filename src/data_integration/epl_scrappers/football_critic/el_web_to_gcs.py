""" EL to extract EPL data and load it to GCS """

import time
import argparse
from datetime import timedelta
import pandas as pd

import utils

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


@task(name='Extract Player Data', retries=2,
      cache_expiration=timedelta(days=1),
      cache_key_fn=task_input_hash)
def extract_player_data(crawl_url: str):
    """Crawl FootballCritic website, extract players
    data, and load it into a Pandas DataFrame

    Args:
    -----
    crawl_url : str
        EPL website

    Returns:
    --------
        Pandas DataFrame with EPL Demographic Data
    """
    # Dictionary to store player info
    player_info = {
        'Player Names': [],
        'Club': [],
        'Nationality': [],
        'Age': [],
        'Position': [],
        'Prefered Foot': [],
        'Height': [],
        'Weight': [],
        'Previous Teams': [],
        'CreatedAt': [],
        'UpdatedAt': []
    }

    driver = webdriver.Chrome()
    # Open web browser
    driver.get(url=crawl_url)

    # Find `Accept All Cookies` btn and click it
    accept_cookies_btn = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, "//button[@class=' css-47sehv' and \
             contains(., 'AGREE')]"))
    )
    accept_cookies_btn.click()

    # Cancel `Push Notifications` message
    accept_cookies_btn = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH,
             "//button[@id='onesignal-slidedown-cancel-button' \
                and contains(., 'Later')]"))
    )
    accept_cookies_btn.click()

    # Wait for `10` secs
    time.sleep(10)

    # Find the total height of the page
    total_height = driver.execute_script(
        "return Math.max( document.body.scrollHeight, "
        "document.body.offsetHeight, "
        "document.documentElement.clientHeight, "
        "document.documentElement.scrollHeight, "
        "document.documentElement.offsetHeight );"
    )

    # Scroll to the middle of the page
    middle_position = total_height // 4
    driver.execute_script(f"window.scrollTo(0, {middle_position});")
    time.sleep(5)

    try:
        page_num = 1
        while page_num <= 22:
            # Find element with player name
            players = driver.find_elements(
                By.XPATH, '//a[@class="only_desktop"]')

            # Loop through all player's information
            for player in players:
                # Find player name
                player_info['Player Names'].append(player.text)
                print(player.text)

                # Get href attribute from `player` element
                href = player.get_attribute('href')

                # Open player info page in a new window
                driver.execute_script(f"window.open('{href}', '_blank');")
                # Switch to the newly opened window
                driver.switch_to.window(driver.window_handles[1])
                # # Navigate to player info page
                time.sleep(5)

                # Find elements with player information
                information = driver.find_element(
                    By.XPATH, '//ul[@class="add-info"]')
                information_list = information.text.split("\n")
                player_info = utils.add_player_details(
                    information_list, player_info)

                # Close the player info window
                driver.close()
                # Switch back to the main window
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(3)

            next_page_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//a[@class="paginate_button next" and \
                     contains(., "Next")]'))
            )
            # Scroll to the element using JavaScript
            driver.execute_script(
                "arguments[0].scrollIntoView(true);", next_page_btn)
            # Click the "Next" button using JavaScript
            driver.execute_script("arguments[0].click();", next_page_btn)
            time.sleep(2)

            page_num += 1

    except exceptions.TimeoutException:
        pass

    return pd.DataFrame(
        data=player_info
    )


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
        to_path=f'epl_player_data_repository/{epl_season}/{data_file_name}.parquet',
        serialization_format='parquet_gzip'
    )


@flow(name='Get Premier League Data')
def etl(url: str, season: str, dataset_file_name: str) -> None:
    """ETL function to extract, transform, and
    load data it into GCS

    Args:
    -----
    url : str
        URL to EPL website
    season : str
        EPL Season
    dataset_file_name : str
        EPL dataset file name

    Returns:
        None
    """

    # Extract data
    data_frame = extract_player_data(crawl_url=url)
    # Load to GCS
    load_to_gcs(data_frame, season, dataset_file_name)


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(
        description='Pass Season Year and Tag'
    )
    arg_parser.add_argument('--season_year', type=str, required=True)
    arg_parser.add_argument('--season_tag', type=str, required=True)

    season = arg_parser.parse_args().season_year
    tag = arg_parser.parse_args().season_tag

    DATASET_FILE_NAME = 'epl_player_data'

    # Construct URL
    web_url = f'https://www.footballcritic.com/premier-league/season-{season}/player-stats/all/2/{tag}'

    etl(url=web_url, season=season, dataset_file_name=DATASET_FILE_NAME)
