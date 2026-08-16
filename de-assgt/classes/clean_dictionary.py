"""
----------------------
Author: Chin Yin Ern |
----------------------

"""

import requests
from bs4 import BeautifulSoup
import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from concurrent.futures import ThreadPoolExecutor
from classes.dataframe_saver import DataFrameSaver

class CleanDefinition:
    @staticmethod
    def clean_definition(text):
        cleaned_text = re.sub(r'[\u0600-\u06FF\u0750-\u077F]', '', text)
        cleaned_text = re.sub(r'[^\w\s\[\].:~-]', '', cleaned_text)
        return cleaned_text.strip()

    @staticmethod
    def fetch_definition(keyword):
        url = f"https://prpm.dbp.gov.my/Cari1?keyword={keyword}"
        try:
            response = requests.get(url, timeout=5)  
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            s = soup.find('div', class_='tab-content')

            if s:
                raw_text = s.get_text(strip=True)
                return CleanDefinition.clean_definition(raw_text)
            else:
                return "-"
        except requests.RequestException:
            return "Error fetching content."

    @staticmethod
    def fetch_definitions_parallel(words, max_workers=10):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(CleanDefinition.fetch_definition, words))
        return results

    @staticmethod
    def fetch_and_save_definitions(spark, input_file, output_file_local, output_file_hdfs):
        input_df = spark.read.csv(input_file, header=True, inferSchema=True)

        unique_words = input_df.select("words").distinct().rdd.flatMap(lambda x: x).collect()

        definitions = CleanDefinition.fetch_definitions_parallel(unique_words, max_workers=10)

        word_definition_map = dict(zip(unique_words, definitions))

        def lookup_definition(word):
            return word_definition_map.get(word, "Definition not found")

        lookup_definition_udf = udf(lookup_definition, StringType())

        output_df = input_df.withColumn("definition", lookup_definition_udf(col("words")))

        output_dir1 = "/home/student/de-assgt/content/"
        file_name = "dictionary.csv"
        DataFrameSaver.save_to_csv(output_df, output_dir1, file_name)

        # Write the output DataFrame back to HDFS
        output_df.coalesce(1).write.csv(output_file_hdfs, header=True, mode="overwrite")
