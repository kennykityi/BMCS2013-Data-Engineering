"""
----------------------
Author: Chin Yin Ern |
----------------------

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class WordLengthAnalysis:

    @staticmethod
    def performAllOperations(input_file, spark):
        df = spark.read.csv(input_file, header=True, inferSchema=True)

        df = df.withColumn("word_length", length(col("words"))).orderBy(col("word_length").desc())

        df = df.withColumn(
            "length_category",
            when(col("word_length") <= 4, "short")
            .when(col("word_length").between(5, 8), "medium")
            .otherwise("long")
        )  
        df.show()

        return df


    @staticmethod
    def calculateOverallAverageWordLength(df, spark):

        df_stats = df.withColumn("char_count_no_spaces", length(regexp_replace(col("words"), " ", ""))) \
                     .withColumn("word_count", size(split(col("words"), " ")))

        total_characters = df_stats.select(sum("char_count_no_spaces").alias("total_characters")).collect()[0]["total_characters"]
        total_words = df_stats.select(sum("word_count").alias("total_words")).collect()[0]["total_words"]

        overall_avg_word_length = total_characters / total_words if total_words > 0 else 0

        print(f"Total Characters (excluding spaces): {total_characters}")
        print(f"Total Words: {total_words}")
        print(f"Overall Average Word Length: {overall_avg_word_length:.2f}")

    @staticmethod
    def calculateHighestWordLength(df, spark):
        """
        Print words with the highest and lowest word lengths.
        """
        max_length = df.agg(max("word_length")).collect()[0][0]
        min_length = df.agg(min("word_length")).collect()[0][0]

        max_length_df = df.filter(col("word_length") == max_length)
        min_length_df = df.filter(col("word_length") == min_length)

        print("Words with maximum length:")
        max_length_df.select("words", "word_length").show()

        print("Words with minimum length:")
        min_length_df.select("words", "word_length").show()




