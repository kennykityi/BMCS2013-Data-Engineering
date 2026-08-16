"""
--------------------------
Author: Kenny Loh Kit Yi |
--------------------------

"""

import requests
from bs4 import BeautifulSoup
from classes.data_crawler import DataCrawler

class WebScraper:
    @staticmethod
    def extract_article_content(content_url):
        response = requests.get(content_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        article_content = soup.find('article')

        for div in article_content.find_all('div'):
            div.decompose()  

        p_tags = article_content.find_all('p', style=False)
        content = ' '.join([p.get_text(strip=True) for p in p_tags])
        return content

    @staticmethod
    def extract_all_articles(web_url, category_links, max_articles):
        article_contents = []
        total_articles = 0

        for category_link in category_links:
            if total_articles >= max_articles:
                break

            print(f"Processing category: {category_link}")
            article_links = DataCrawler.extract_article_links(web_url, category_link)
            for article_link in article_links:
                if total_articles >= max_articles:
                    break

                content = WebScraper.extract_article_content(article_link)
                if content:
                    article_contents.append(content)
                    total_articles += 1

        return article_contents
