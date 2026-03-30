from sqlite3.dbapi2 import connect
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from minio import Minio
from dotenv import load_dotenv
from utils.logger import get_logger, log_step

load_dotenv()

logger = get_logger(__name__)

# Extraire en faisant le scraping des livres
base_url_book = "https://books.toscrape.com/"
base_url_csv = "/opt/airflow/data/Books.csv"
base_bd_url = "/opt/airflow/data/books_database.db"

def _fetch_page_book(url: str) -> BeautifulSoup:
    res = requests.get(url)
    return BeautifulSoup(res.text, "html.parser")

def _book(article: BeautifulSoup):
    RATING_MAP = {
        "One": 1,
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5,
    }

    # title
    title = article.find('h3').find('a')
    title = title["title"]

    # image
    image = article.find('img')["src"]

    # price
    price = article.find('p', class_='price_color').text

    # availability
    availability_tag = article.find("p", class_="instock")
    availability = 1 if availability_tag is not None else 0

    # rating
    rating = article.find('p', class_='star-rating')
    rating = rating.get('class')[1]
    rating = RATING_MAP[rating]

    return {
        "title": title,
        "image": image,
        "price": price,
        "availability": availability,
        "rating": rating
    }

def extract_book(raw_path: str) -> pd.DataFrame:
    with log_step(logger, "extract_book", raw_path=raw_path):
        all_books = []

        with log_step(logger, "extract_book.fetch_page", url=base_url_book):
            soup = _fetch_page_book(base_url_book)

        articles = soup.find_all('article', class_="product_pod")
        logger.info("extract_book found_articles=%s", len(articles))

        # Scraping: on log la progression sans spam (volume généralement faible ici).
        for idx, article in enumerate(articles, start=1):
            book = _book(article)
            all_books.append(book)
            if idx == 1 or idx % 10 == 0 or idx == len(articles):
                logger.info("extract_book progress=%s/%s", idx, len(articles))

        df = pd.DataFrame(all_books)
        out_path = os.path.join(raw_path, "books_scraping.parquet")
        logger.info("extract_book saving_parquet out_path=%s shape=%s", out_path, df.shape)
        df.to_parquet(out_path)
        return df

# Extraire en faisant l'extraction des livres depuis un fichier csv
def extract_csv(raw_path: str) -> pd.DataFrame:
    with log_step(logger, "extract_csv", raw_path=raw_path, source=base_url_csv):
        df = pd.read_csv(base_url_csv, dtype={"Year-Of-Publication": "string"})
        out_path = os.path.join(raw_path, "books_csv.parquet")
        logger.info("extract_csv saving_parquet out_path=%s shape=%s", out_path, df.shape)
        df.to_parquet(out_path)
        return df
    
def extract_sqlite(raw_path: str) -> pd.DataFrame:
    import sqlite3
    with log_step(logger, "extract_sqlite", raw_path=raw_path, db_path=base_bd_url):
        query = 'SELECT * FROM books'
        with log_step(logger, "extract_sqlite.query", query=query):
            conn = sqlite3.connect(base_bd_url)
            df = pd.read_sql(query, conn)
            conn.close()

        out_path = os.path.join(raw_path, "books_sqlite.parquet")
        logger.info("extract_sqlite saving_parquet out_path=%s shape=%s", out_path, df.shape)
        df.to_parquet(out_path)
        return df

def transform_books(_books: pd.DataFrame, _books_csv: pd.DataFrame, _books_sqlite: pd.DataFrame) -> pd.DataFrame:
    with log_step(
        logger,
        "transform_books",
        books_shape=getattr(_books, "shape", None),
        books_csv_shape=getattr(_books_csv, "shape", None),
        books_sqlite_shape=getattr(_books_sqlite, "shape", None),
    ):
        # Normaliser les colonnes price de _books et _books_sqlite
        with log_step(logger, "transform_books.normalize_price"):
            _books["price"] = pd.to_numeric(_books["price"].str.replace("Â£", ""))
            _books_sqlite["price"] = pd.to_numeric(_books_sqlite["price"])

        # Supprimer les colonnes dont on a pas besoin
        with log_step(logger, "transform_books.drop_columns"):
            _books_csv.drop(columns=['Image-URL-M', 'Image-URL-L'], inplace=True)
            _books_sqlite.drop(columns=[
                'format', 'weight_grams', 'dimensions', 'edition', 'series', 'volume',
                'original_language', 'translator', 'cover_type', 'age_group',
                'bestseller', 'award_winner', 'created_at'
            ], inplace=True)

        # Renommer les colonnes
        with log_step(logger, "transform_books.rename_columns"):
            _books_csv.rename(columns={
                'Book-Title': 'title',
                'Book-Author': 'author',
                'Year-Of-Publication': 'publication_year',
                'Publisher': 'publisher',
                'Image-URL-S': 'image'
            }, inplace=True)
            _books_sqlite.rename(columns={'in_stock': 'availability'}, inplace=True)

        # Créer une colonne id dans les df _books et _books_csv
        with log_step(logger, "transform_books.create_ids"):
            last_id = _books_sqlite["id"].max()
            _books["id"] = range(last_id + 1, last_id + len(_books) + 1)
            _books_csv["id"] = range(
                _books["id"].max() + 1,
                _books["id"].max() + 1 + len(_books_csv),
            )

        # Fusionner les dataframes
        with log_step(logger, "transform_books.concat"):
            all_books = pd.concat([_books, _books_csv, _books_sqlite])

        # Convertir les colonnes dans les bons types
        with log_step(logger, "transform_books.cast_types"):
            all_books["price"] = pd.to_numeric(all_books["price"], errors="coerce")
            all_books["availability"] = pd.to_numeric(all_books["availability"], errors="coerce")
            all_books["rating"] = pd.to_numeric(all_books["rating"], errors="coerce")
            all_books["publication_year"] = pd.to_numeric(all_books["publication_year"], errors="coerce")

        logger.info("transform_books done shape=%s", all_books.shape)
        return all_books

def save_to_parquet_and_upload(all_books_path: str):
    with log_step(logger, "save_to_parquet_and_upload", all_books_path=all_books_path):
        client = Minio(
            os.getenv("MINIO_ENDPOINT_DOCKER"),
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            secure=False,
        )

        bucket_name = "books"

        with log_step(logger, "minio.ensure_bucket", bucket_name=bucket_name):
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)

        df = pd.read_parquet(all_books_path)
        logger.info("save_to_parquet_and_upload loaded_parquet shape=%s", df.shape)
        df_grouped = df.groupby("publication_year")

        # Upload "1 parquet par année" (comportement inchangé), mais log de progression.
        for year, group in df_grouped:
            folder = f'publication_year_{int(year)}'
            local_dir = "/opt/airflow/data/parquet/books"
            local_path = f'{local_dir}/{folder}.parquet'
            object_name = f'books/{folder}.parquet'

            logger.info(
                "upload_by_year start year=%s group_shape=%s object_name=%s local_path=%s",
                year,
                group.shape,
                object_name,
                local_path,
            )

            os.makedirs(local_dir, exist_ok=True)

            # Sauvegarder localement le parquet
            group.to_parquet(local_path, engine='pyarrow')

            client.fput_object(bucket_name, object_name, local_path)
            logger.info("upload_by_year success year=%s object_name=%s", year, object_name)

if __name__ == "__main__":
    # Extract
    books = extract_book()
    books_csv = extract_csv()
    books_sqlite = extract_sqlite()

    # Transform
    all_books = transform_books(books, books_csv, books_sqlite)

    # Load
    save_to_parquet_and_upload(all_books)