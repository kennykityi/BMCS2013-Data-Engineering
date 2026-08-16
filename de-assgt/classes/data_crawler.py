"""
--------------------------
Author: Kenny Loh Kit Yi |
--------------------------

"""

import requests
from bs4 import BeautifulSoup

class DataCrawler:
    @staticmethod
    def extract_category_links(web_url):
        response = requests.get(web_url)
        category_links = []

        soup = BeautifulSoup(response.text, 'html.parser')
        menu_buttons = soup.find('div', class_="menuButtons")
        navbar = menu_buttons.find('div', id="navbarContent")
        berita_section = navbar.find('a', string="Berita")
        subcategories = berita_section.find_next('ul')
        subcategory_items = subcategories.find_all('a')
        
        for subcategory in subcategory_items:
            subcategory_name = subcategory.get_text(strip=True).lower().replace(" ", "-")
            category_links.append(f"{web_url}/{subcategory_name}")
        
        return category_links

    @staticmethod
    def extract_article_links(web_url, article_url):
        response = requests.get(article_url)
        article_links = set()

        soup = BeautifulSoup(response.text, 'html.parser')
        article_links = {
            link['href'] for link in soup.find_all('a', href=True)
            if link['href'].startswith(f"{web_url}/article/")
        }

        return list(article_links)
