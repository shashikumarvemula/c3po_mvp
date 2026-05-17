import json
import dask.dataframe as dd
import pandas as pd

class BYODFilesLoader:
    BASE_PATH = "./BYOD/"
    INPUT_FILES_PATH = BASE_PATH + "Input_Files/"
    DATA_FILES_PATH = BASE_PATH + "Data_Files/"

    @staticmethod
    def load_json_file(path):
        """Loads a JSON file from the given path."""
        with open(path, 'r') as file:
            return json.load(file)

    @staticmethod
    def load_text_file(path):
        """Loads a text file as a string."""
        with open(path, 'r') as file:
            return file.read()

    @staticmethod
    def load_xl_file(path):
        """Loads an xl file and returns it as a pandas DataFrame"""
        return pd.read_excel(path)
    

    @classmethod
    def load_instructions(cls):

        """Loads all instruction files from Input_Files/."""
        instructions = {
            "byod_system_prompt":cls.load_text_file(cls.INPUT_FILES_PATH + "byod_system_prompt.txt"),
        }
        print("Loaded  byod prompt")
        return instructions