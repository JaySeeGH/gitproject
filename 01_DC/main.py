import os
from pathlib import Path
os.chdir(Path(__file__).parent)

from src.data_cleaner import DataCleaner

def main():
    cleaner = DataCleaner('config/cleaning_config.json',  dev=True)
    cleaner.delete_files() # if delete
    cleaned_data = cleaner.clean_data("./data/my_data_test3.csv","cleaned/", "my_data_output_test5.csv",sep=",")
    if cleaned_data:
        print("Data cleaned successfully")

if __name__ == "__main__":
    main()