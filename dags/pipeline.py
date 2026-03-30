import sys
sys.path.append("/opt/airflow/src")


from datetime import datetime, timedelta
import pandas as pd
from main import extract_book, extract_csv, extract_sqlite, transform_books, save_to_parquet_and_upload
import os
from airflow.decorators import task, dag
from utils.logger import get_logger

logger = get_logger(__name__)

# Tous les chemins
RAW_PATH = "/opt/airflow/data/raw"
PROCESSED_PATH = "/opt/airflow/data/processed"
PARQUET_PATH = "/opt/airflow/data/parquet"

# Tous les fichiers
BOOKS_SCRAPING_PATH = os.path.join(RAW_PATH, "books_scraping.parquet")
BOOKS_CSV_PATH = os.path.join(RAW_PATH, "books_csv.parquet")
BOOKS_SQLITE_PATH = os.path.join(RAW_PATH, "books_sqlite.parquet")
ALL_BOOKS_PATH = os.path.join(PROCESSED_PATH, "all_books.parquet")

default_args = {
    'owner': 'Kathy Déwama SATERA',
    'start_date': datetime.now(),
    # 'email': ['kathysatera@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id="ETL_pipeline",
    description="Pipeline d'extraction, transformation et chargement des données des livres",
    schedule=None,
    default_args=default_args
)
def run_pipeline():
    @task
    def extract():
        os.makedirs(RAW_PATH, exist_ok=True)

        logger.info("DAG extract start RAW_PATH=%s", RAW_PATH)

        books = extract_book(RAW_PATH)
        logger.info("DAG extract books_scraping shape=%s", books.shape)

        books_csv = extract_csv(RAW_PATH)
        logger.info("DAG extract books_csv shape=%s", books_csv.shape)

        books_sqlite = extract_sqlite(RAW_PATH)
        logger.info("DAG extract books_sqlite shape=%s", books_sqlite.shape)

        books.to_parquet(BOOKS_SCRAPING_PATH)
        books_csv.to_parquet(BOOKS_CSV_PATH)
        books_sqlite.to_parquet(BOOKS_SQLITE_PATH)

        logger.info(
            "DAG extract done output=%s,%s,%s",
            BOOKS_SCRAPING_PATH,
            BOOKS_CSV_PATH,
            BOOKS_SQLITE_PATH,
        )

    @task
    def transform():
        os.makedirs(PROCESSED_PATH, exist_ok=True)

        logger.info("DAG transform start reading parquet")
        books = pd.read_parquet(BOOKS_SCRAPING_PATH)
        books_csv = pd.read_parquet(BOOKS_CSV_PATH)
        books_sqlite = pd.read_parquet(BOOKS_SQLITE_PATH)

        logger.info(
            "DAG transform loaded shapes books=%s books_csv=%s books_sqlite=%s",
            books.shape,
            books_csv.shape,
            books_sqlite.shape,
        )

        all_books = transform_books(books, books_csv, books_sqlite)

        all_books.to_parquet(ALL_BOOKS_PATH, engine='pyarrow')
        logger.info("DAG transform done all_books shape=%s out_path=%s", all_books.shape, ALL_BOOKS_PATH)

    @task
    def load():
        logger.info("DAG load start all_books_path=%s", ALL_BOOKS_PATH)
        save_to_parquet_and_upload(ALL_BOOKS_PATH)
        logger.info("DAG load done")

    extract() >> transform() >> load()

run_pipeline()