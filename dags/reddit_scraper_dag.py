from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# -- Path Setup: Ensure Airflow can find your scraper module --
# This might need adjustment based on your Airflow setup and project root.
# It assumes your project root is the parent directory of 'dags' and 'scraper'.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from scraper.continuous_scraper import scrape_recent_posts

# --- DAG Definition ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'reddit_ethtrader_scraper',
    default_args=default_args,
    description='Scrape r/ethtrader for new posts every minute.',
    schedule='*/1 * * * *',  # This cron expression means "run every minute"
    catchup=False,
    tags=['reddit', 'scraping', 'ethtrader'],
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_recent_reddit_posts',
        python_callable=scrape_recent_posts,
    ) 