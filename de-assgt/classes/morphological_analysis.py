"""
----------------------
Author: Chan Jin Wei |
----------------------

"""

from pyspark.sql.functions import *
import re
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
from pyspark.sql.types import StringType
from classes.dataframe_saver import DataFrameSaver


class MorphologicalAnalysis:

    @staticmethod
    def fetch_kata_terbitan(word):
        url = f"https://prpm.dbp.gov.my/Cari1?keyword={word}"
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            kata_terbitan_section = soup.find('b', string="Kata Terbitan : ")

            if kata_terbitan_section:
                # Find all <a> tags under the <i> tag that follows <b>Kata Terbitan :</b>
                kata_terbitan_links = kata_terbitan_section.find_next('i').find_all('a')

                # Extract text from each link
                kata_terbitan_list = [link.get_text(strip=True) for link in kata_terbitan_links]

                # Return comma-separated list of 'kata terbitan' or a fallback message
                return ', '.join(kata_terbitan_list) if kata_terbitan_list else "No kata terbitan found"
            else:
                return "-"
        else:
            return f"Failed to fetch URL, Status Code: {response.status_code}"

    @staticmethod
    def clean_numbers(text):
        return re.sub(r'\d+', '', text).strip()

    @staticmethod
    def fetch_kata_dasar(word):
        url = f"https://prpm.dbp.gov.my/Cari1?keyword={word}"
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Locate all <span> elements with 'font-weight:bold;'
            kata_dasar = soup.find_all('span', style=lambda value: value and 'font-weight:bold;' in value)

            # If span is found, return its cleaned text directly
            if len(kata_dasar) == 1:
                return MorphologicalAnalysis.clean_numbers(kata_dasar[0].get_text(strip=True))

            if not kata_dasar:  # No matching spans
                return "-"
        else:
            return f"Failed to fetch URL, Status Code: {response.status_code}"

    @staticmethod
    def process_dataframe(df):
        """
        Process the DataFrame to add kata_dasar, kata_terbitan, and num_variations columns.
        """
        kata_dasar_udf = udf(lambda word: MorphologicalAnalysis.fetch_kata_dasar(word), StringType())
        kata_terbitan_udf = udf(lambda word: MorphologicalAnalysis.fetch_kata_terbitan(word), StringType())

        df = df.withColumn('kata_dasar', kata_dasar_udf(col('words')))
        df = df.withColumn('kata_terbitan', kata_terbitan_udf(col('words')))

        df = df.withColumn(
            'num_variations',
            when((col('kata_terbitan').isNotNull()) & (col('kata_terbitan') != '-'), size(split(col('kata_terbitan'), ',')))
            .otherwise(0)
        )

        return df

    @staticmethod
    def summary(df):
        """
        Generates a summary of the processed DataFrame.
        Includes:
        - Counts of words with/without 'kata_terbitan'
        - Distribution of 'num_variations'
        - Words with the maximum number of derivations
        - Visualization of derivation distribution
        """
        # Clean and trim whitespace for 'kata_terbitan' column
        df = df.withColumn("kata_terbitan", trim(col("kata_terbitan")))

        # Count words with null and non-null 'kata_terbitan'
        null_count = df.filter(col("kata_terbitan") == "-").count()
        non_null_count = df.filter(col("kata_terbitan") != "-").count()

        # Add a column for the number of derivations (split by ',' if valid)
        derivation_distribution = (
            df.groupBy("num_variations")
            .agg(count("*").alias("frequency"))
            .orderBy("num_variations")
        )

        # Visualization using Matplotlib
        distribution_data = derivation_distribution.collect()
        x_values = [row["num_variations"] for row in distribution_data]
        y_values = [row["frequency"] for row in distribution_data]

        plt.figure(figsize=(10, 6))
        plt.bar(x_values, y_values, color='skyblue')
        plt.title("Distribution of 'kata_terbitan' Derivations")
        plt.xlabel("Number of Derivations")
        plt.ylabel("Frequency")
        plt.xticks(x_values)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()

        max_derivations = df.agg(max("num_variations")).collect()[0][0]

        max_derivations_df = df.filter(col("num_variations") == max_derivations)

        print(f"Number of words with no 'kata_terbitan' : {null_count}")
        print(f"Number of words with 'kata_terbitan' : {non_null_count}\n")

        if max_derivations > 0:
            print(f"Words with the highest number of 'kata_terbitan' derivations ({max_derivations}):")
            max_derivations_df.select("words", "kata_terbitan", "num_variations").show(truncate=False)
        else:
            print("No valid derivations found.")

        print("Original data frame:")
        df.show()
