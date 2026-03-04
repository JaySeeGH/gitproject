__title__ = "Data Cleaning Application"
__author__ = "Jens"
__version_ = "0.1.1"
__doc__ = """
The Application is designed to clean an input CSV and export an new cleaned CSV file
Viele Schritte, um eine CSV zu bereinigen
"""

import json
import pandas as pd
import os
from pathlib import Path

class DataCleaner:
    """
    The DataCleaner object clean and log Data from CSV files with a config file
    """
    def __init__(self, config_file="./config/" + "cleaning_config.json",*,dev=False,run=True):
        self.config = self.load_config(Path(config_file))
        self.input_file = "./data/my_data.csv"
        self.output_folder = "./cleaned/"
        self.output_file = "my_data_output.csv"
        self.dev = dev
        self.run = run

    def load_config(self, config_file):
        with open(config_file, "r", encoding="utf-8") as file:
            return json.load(file)

    def write_file_logs(self, filename, content, folder="./logs/"):
        Path(folder + filename + ".txt").write_text(str(content), encoding="utf-8")

    def delete_files(self, folder="./logs/"):
        for root, dirs, files in os.walk(Path(folder)):
            for filename in files:
                #if os.path.isfile("./logs/" + filename + ".txt"):
                os.remove(folder + filename)

    def make_dir(self, folder):
        Path(folder).mkdir(parents=True, exist_ok=True)

    def clean_data(self, input_file:str,output_folder:str,output_file:str,*,sep:str=","):
        """

        :param input_file:
        :param output_folder:
        :param output_file:
        :param sep:
        :return:
        """
        config = self.config
        # self.input_file = input_file
        output_file = output_folder + output_file

        df = pd.read_csv(input_file, sep=sep)
        if self.dev:
            print("1:",df.shape)
        if config["drop_empty_cols"]:
            df = self.drop_empty_cols(df,config["logging"]["drop_empty_cols"])
        if self.dev:
            print("2:",df.shape)

        if config["drop_empty_rows"]:
            df = self.drop_empty_rows(df,config["logging"]["drop_empty_rows"])
        if self.dev:
            print("3:",df.shape)

        if config["split_cols"]:
            df = self.split_cols(df,config["split_col_source"],config["split_cols_dest_list"], config["split_cols_dest_index"],config["split_cols_sep"],config["logging"]["split_cols"])
        if self.dev:
            print("3:",df.shape)

        if config["drop_na"]:
            df = self.drop_na(df,config["logging"]["drop_na"])
        if self.dev:
            print("4:", df.shape)

        if config["remove_duplicates"]:
            df = self.remove_duplicates(df,config["logging"]["remove_duplicates"])
        if self.dev:
            print("5:", df.shape)

        if config["data_type_corrections"]:
            df = self.data_type_corrections(df,config["logging"]["data_type_corrections"])
        if self.dev:
            print("6:", df.shape)

        if config["fill_mean"]:
            df = self.fill_mean(df,config["logging"]["fill_mean"])
        if self.dev:
            print("7:", df.shape)
        if self.run:
            os.makedirs(output_folder, exist_ok=True)
            df.to_csv(output_file,index=False)
            return True
        else:
            print("TEST-MODE")



    # ========================
    # Pipeline Methoden
    # ========================
    def drop_empty_cols(self,df,logs=False):
        leere_spalten = df.columns[df.isnull().all()].tolist()
        # print(leere_spalten)
        if logs and leere_spalten:
            self.write_file_logs("drop_empty_cols", leere_spalten)
        df_cleaned = df.dropna(axis=1, how='all')
        return df_cleaned

    def split_cols(self,df,split_col_source,split_cols_dest_list,split_cols_dest_index,split_sep=",",logs=False):
        # Stufe 1 - Spalte wird gesplitted und als Einzelspalten angehängt - okay
        # Stufe 2 - Es wird nur die gewünschte Spalte zusätzlich erzeugt und der Rest wird wieder zusammengesetzt - offen

        # Stufe 2
        # Spalte EXT, die übrig bleiben soll, speichern
        # andere Spalten OTHERS mergen
        #


        # Anzahl der Spalten ermitteln
        split_example = str(df[split_col_source][2])
        splitting_example = split_example.split(split_sep)
        count_new_cols = len(splitting_example)
        print(count_new_cols)
        #df_splitted = df[split_col_source]


        # Stufe 1
        df[split_cols_dest_list] = df[split_col_source].replace("\"","").replace(r'\s*(.*?)\s*', r'\1', regex=True).str.split(split_sep, expand=True)

        if logs:
            self.write_file_logs("split_cols", count_new_cols) # df[['Purchase Address', 'ZIP']]

        return df

    def drop_empty_rows(self, df, logs=False):
        empty_rows = df[df.isna().all(axis=1)]
        #print(empty_rows)
        if logs:
            self.write_file_logs("drop_empty_rows", empty_rows)
        df.dropna(how='all', inplace=True)
        return df

    def drop_na(self,df,logs=False):
        rows_with_nan = df[df.isnull().any(axis=1)]
        df_cleaned = df.dropna()

        if logs:
            self.write_file_logs("drop_na", rows_with_nan)
        return df_cleaned

    def remove_duplicates(self,df,logs=False):
        duplicates = df[df.duplicated()]
        if logs:
            self.write_file_logs("remove_duplicates", duplicates)
        df_clean = df.drop_duplicates()
        return df_clean

    def data_type_corrections(self,df,logs=False):
        if logs:
            self.write_file_logs("data_type_corrections", "XXXXXXX")
        return df

    def fill_mean(self,df,logs=False):

        if logs:
            self.write_file_logs("fill_mean", "XXXXXXX")
        return df

if __name__ == "__main__":
    cleaner = DataCleaner('../config/cleaning_config.json')
    cleaner.delete_files('../logs/')
    cleaner.delete_files('../cleaned/')