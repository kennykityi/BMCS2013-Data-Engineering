"""
--------------------------
Author: Kelly Tan Jie Li |
--------------------------

"""

import os
import shutil

class DataFrameSaver:
    
    @staticmethod
    def save_to_csv(df, output_dir, file_name="output.csv"):
        
        temp_dir = f"{output_dir}_temp"
        os.makedirs(output_dir, exist_ok=True)  

        df.coalesce(1).write.csv(f"file://{temp_dir}", header=True, mode="overwrite")

        for file in os.listdir(temp_dir):
            if file.startswith("part-") and file.endswith(".csv"):
                shutil.move(os.path.join(temp_dir, file), os.path.join(output_dir, file_name))
                break

        shutil.rmtree(temp_dir)

    @staticmethod
    def save_to_csv_1(df, output_dir, file_name="output.csv"):
        
        temp_dir = f"{output_dir}_temp"
        os.makedirs(output_dir, exist_ok=True)  

        df.coalesce(1).write.option("sep", "\t").option("quote", "\"").csv(f"file://{temp_dir}", header=True, mode="overwrite")

        for file in os.listdir(temp_dir):
            if file.startswith("part-") and file.endswith(".csv"):
                shutil.move(os.path.join(temp_dir, file), os.path.join(output_dir, file_name))
                break

        shutil.rmtree(temp_dir)