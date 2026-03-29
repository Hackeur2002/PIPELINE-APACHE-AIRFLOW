import requests
from bs4 import BeautifulSoup
import pandas as pd

# Extraire en faisant le scraping des livres
base_url_book = "https://books.toscrape.com/"
base_url_csv = "../data/Books.csv"

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

def extract_csv() -> pd.DataFrame:
    all_book_csv = []
    df = pd.read_csv(base_url_csv)
    return df
    
if __name__ == "__main__":
    books = extract_book()
    books_csv = extract_csv()
    print("Books from url book:")
    print(books)
    print("Books from csv:")
    print(books_csv.head())
    print(books_csv.shape)
    print(books_csv.columns)