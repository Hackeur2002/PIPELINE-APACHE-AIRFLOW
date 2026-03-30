from sqlite3.dbapi2 import connect
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

# Extraire en faisant le scraping des livres
base_url_book = "https://books.toscrape.com/"
base_url_csv = "../data/Books.csv"
base_bd_url = "../data/books_database.db"

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

def extract_book():
    all_books = []
    soup = _fetch_page_book(base_url_book)
    articles = soup.find_all('article', class_="product_pod")

    for article in articles:
        book = _book(article)
        all_books.append(book)
    all_books = pd.DataFrame(all_books)
    return all_books

# Extraire en faisant l'extraction des livres depuis un fichier csv
def extract_csv() -> pd.DataFrame:
    all_book_csv = []
    df = pd.read_csv(base_url_csv)
    return df
    
def extract_sqlite() -> pd.DataFrame:
    import sqlite3
    conn = sqlite3.connect(base_bd_url)
    query = 'SELECT * FROM books'
    df = pd.read_sql(query, conn)
    return df

def transform_books(_books: pd.DataFrame, _books_csv: pd.DataFrame, _books_sqlite: pd.DataFrame) -> pd.DataFrame:
    # Normaliser les colonnes price de _books et _books_sqlite
    _books["price"] = pd.to_numeric(_books["price"].str.replace("Â£", ""))
    _books_sqlite["price"] = pd.to_numeric(_books_sqlite["price"])

    # Supprimer les colonnes dont on a pas besoin
    _books_csv.drop(columns=['Image-URL-M', 'Image-URL-L'], inplace=True)
    _books_sqlite.drop(columns=['format', 'weight_grams', 'dimensions', 'edition', 'series', 'volume', 'original_language', 'translator', 'cover_type', 'age_group', 'bestseller', 'award_winner', 'created_at'], inplace=True)
    
    # Renommer les colonnes
    _books_csv.rename(columns={'Book-Title': 'title', 'Book-Author': 'author', 'Year-Of-Publication': 'publication_year', 'Publisher': 'publisher', 'Image-URL-S': 'image'}, inplace=True)
    _books_sqlite.rename(columns={'in_stock': 'availability'}, inplace=True)

    # Créer une colonne id dans les df _books et _books_csv
    last_id = _books_sqlite["id"].max()
    _books["id"] = range(last_id + 1, last_id + len(_books) + 1)
    _books_csv["id"] = range(_books["id"].max() + 1, _books["id"].max() + 1 + len(_books_csv))

    # Fusionner les dataframes
    all_books = pd.concat([_books, _books_csv, _books_sqlite])

    # Convertir les colonnes dans les bons types
    all_books["price"] = pd.to_numeric(all_books["price"], errors="coerce")
    all_books["availability"] = pd.to_numeric(all_books["availability"], errors="coerce")
    all_books["rating"] = pd.to_numeric(all_books["rating"], errors="coerce")
    all_books["publication_year"] = pd.to_numeric(all_books["publication_year"], errors="coerce")

    return all_books

def save_to_parquet_and_upload(df: pd.DataFrame):
    client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )

    bucket_name = "books"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    df_grouped = df.groupby("publication_year")

    for year, group in df_grouped:
        print(year)
        print(group.shape)

        folder = f'publication_year_{int(year)}'
        local_path = f'../data/parquet/books/{folder}.parquet'

        os.makedirs("../data/parquet/books", exist_ok=True)

        # Sauvegarder localement le parquet
        group.to_parquet(local_path, engine='pyarrow')

        object_name = f'books/{folder}.parquet'

        client.fput_object(bucket_name, object_name, local_path)
        print(f"Uploaded {object_name} to {bucket_name} Ok")

if __name__ == "__main__":
    # Extract
    books = extract_book()
    books_csv = extract_csv()
    books_sqlite = extract_sqlite()

    # Transform
    all_books = transform_books(books, books_csv, books_sqlite)

    # Load
    save_to_parquet_and_upload(all_books)