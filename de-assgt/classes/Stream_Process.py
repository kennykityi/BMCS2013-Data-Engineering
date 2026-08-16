"""
--------------------------
Author: Kelly Tan Jie Li |
--------------------------

"""

import os
import shutil
from data_crawler import DataCrawler
from dataframe_saver import DataFrameSaver
from web_scraper import WebScraper
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

class stream_process:
    
    @staticmethod
    def api_crawl(no_article):
        spark = SparkSession \
            .builder \
            .appName("Assignment") \
            .getOrCreate()
        
        web_url = "https://www.sinarharian.com.my"
        category_links = DataCrawler.extract_category_links(web_url)
        max_articles = no_article
        
        articles = WebScraper.extract_all_articles(web_url, category_links, max_articles)
        df = spark.createDataFrame([(article,) for article in articles], ["article"])
        
        output_dir = "/home/student/de-assgt/content"
        file_name = "articles.csv"
        DataFrameSaver.save_to_csv(df, output_dir, file_name)
        print("Article content saved as articles.csv (local)")
        
        hdfs_path = "articles"
        df.coalesce(1).write.csv(hdfs_path, header=True, mode="overwrite")
        print("Article content has been stored in hdfs")
        
        # Generate the message for the specific article
        if no_article <= df.count():
            article_content = df.collect()[no_article - 1].article
            truncated_content = " ".join(article_content.split()[:100])
            message = (
                f"Article {no_article}:\n"
                f"{truncated_content}...\n" 
            )
        else:
            message = "---"
        
        spark.stop()
        return message






        
    