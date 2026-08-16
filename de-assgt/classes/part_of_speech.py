"""
--------------------------
Author: Kelly Tan Jie Li |
--------------------------

"""

import os
import requests
from bs4 import BeautifulSoup
import csv
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, desc
from classes.dataframe_saver import DataFrameSaver

class FindPartOfSpeech:
    
    @staticmethod
    def get_content_from_page(base_url, word):
        url = f"{base_url}{word}"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"Failed to fetch {url}, Status Code: {response.status_code}")
            return "-"
        
        soup = BeautifulSoup(response.content, 'html.parser')
        pos_tags = []  
        
        try:
            no_info_tag = soup.find('b', string=lambda text: text and "Tiada maklumat tesaurus" in text)
            if no_info_tag:
                return "-"
            
            types = set()
            
            for font in soup.find_all('font', color='blue'):
                italic = font.find('i')
                if italic:
                    types.add(italic.get_text(strip=True).rstrip(',').rstrip(':'))
            
            for italic in soup.find_all('i'):
                font = italic.find('font', color='blue')
                if font:
                    types.add(font.get_text(strip=True).rstrip(',').rstrip(':'))
            
            pos_tags = sorted(types)
        
        except Exception as e:
            print(f"Error parsing word '{word}': {e}")
        
        return pos_tags

    @staticmethod
    def process_pos(input_file, base_url, output_dir, file_name, spark):
        os.makedirs(os.path.dirname(input_file), exist_ok=True)

        df = spark.read.csv(input_file, header=True, inferSchema=True)

        rows = df.collect()
        header = ['words', 'definition', 'pos_tag']
        data = []

        for row in rows:
            words = row['words']
            definition = row['definition'] if row['definition'] else "-"
            pos_tags = FindPartOfSpeech.get_content_from_page(base_url, words)

            if pos_tags:
                for pos in pos_tags:
                    data.append([words, definition, pos])
            else:
                data.append([words, definition, "-"])

        df = spark.createDataFrame(data, schema=header)

        pos_counts = df.groupBy("pos_tag").agg(count("*").alias("count"))
        filtered_pos_counts = pos_counts.filter(col("pos_tag") != "-")

        max_count = filtered_pos_counts.agg({"count": "max"}).collect()[0][0]
        min_count = filtered_pos_counts.agg({"count": "min"}).collect()[0][0]

        pos_counts_with_freq = pos_counts.withColumn(
            "freq_of_use",
            when(col("pos_tag") == "-", lit("-"))  
            .when(col("count") == max_count, lit("High"))
            .when(col("count") == min_count, lit("Low"))
            .otherwise(lit("Moderate"))
        )

        df_with_freq = df.join(
            pos_counts_with_freq.select("pos_tag", "freq_of_use"),
            on="pos_tag",
            how="left"
        )

        final_columns = header + ["freq_of_use"]
        df_with_freq_ordered = df_with_freq.select(*final_columns)
        local_output_file = os.path.join(output_dir, file_name)
        DataFrameSaver.save_to_csv(df_with_freq_ordered, output_dir, file_name)
        
        return df_with_freq_ordered


