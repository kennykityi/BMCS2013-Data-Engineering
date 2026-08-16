"""
----------------------
Author: Chan Jin Wei |
----------------------

"""

from pyspark.sql.functions import *

class DataCleaner:

    @staticmethod
    def csvToDataFrame(csv_file_path, spark):
        
        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

        return df

    @staticmethod
    def remove_numbers(df, column):
        
        return df.withColumn(column, regexp_replace(col(column), r'\d', " "))
    
    @staticmethod
    def convert_to_lowercase(df, column):
        
        return df.withColumn(column, lower(col(column)))
        
    @staticmethod
    def remove_standalone_characters(df, column):
        return df.withColumn(
            column,
            regexp_replace(
            col(column),
            r'(?<!\w)(\b\w\b)(?!\w)',  
            " "
            )
        )

    @staticmethod
    def remove_words_in_parentheses(df, column):
        return df.withColumn(
            column,
            regexp_replace(col(column), r'\([^\)]*\)', " ")
        )
        
    @staticmethod
    def remove_delimiter(df, column, delimiters):
        print(f"Removing delimiters: {delimiters} from column: {column}")

        pattern = f"[{delimiters}]"
        return df.withColumn(column, regexp_replace(col(column), pattern, " "))

    @staticmethod    
    def smart_remove_delimiter(df, column, delimiters):
        pattern = f"(^[{delimiters}]|[{delimiters}]$|(?<!\\w)[{delimiters}]+(?!\\w))"
    
        df = df.withColumn(column, regexp_replace(col(column), pattern, " "))
    
        return df

    @staticmethod    
    def remove_hyphen_from_prefix(df, column, prefix="ke-"):
    
        pattern = f"\\b{prefix}"
    
        replacement = prefix.replace("-", "")
    
        df = df.withColumn(column, regexp_replace(col(column), pattern, replacement))
    
        return df

    @staticmethod
    def normalize_spaces(df, column):
        return df.withColumn(
            column,
            trim(regexp_replace(col(column), r'\s+', ' '))
        )

    @staticmethod
    def remove_duplicates(df, column):
        if column:
            return df.dropDuplicates([column])
        else:
            return df.dropDuplicates()
