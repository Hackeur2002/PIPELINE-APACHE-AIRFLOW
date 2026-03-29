from sqlite3.dbapi2 import connect
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Extraire en faisant le scraping des livres
base_url_book = "https://books.toscrape.com/"
base_url_csv = "../data/Books.csv"
base_bd_url = "../data/books_database.db"

def _fetch_page_book(url: str) -> BeautifulSoup:
    res = requests.get(url)
    return BeautifulSoup(res.text, "html.parser")

def _book(article: BeautifulSoup):
    title = article.find('h3').find('a')
    title = title["title"]
    return {
        "title": title
    }

def extract_book():
    all_books = []
    soup = _fetch_page_book(base_url_book)
    articles = soup.find_all('article', class_="product_pod")

    for article in articles:
        book = _book(article)
        all_books.append(book)
    
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




if __name__ == "__main__":
    books = extract_book()
    books_csv = extract_csv()
    books_sqlite = extract_sqlite()
    print("Books from url book:")
    print(books)
    print("Books from csv:")
    print(books_csv.head())
    print(books_csv.shape)
    print(books_csv.columns)
    print("Books from sqlite:")
    print(books_sqlite.head())
    print(books_sqlite.shape)
    print(books_sqlite.columns)