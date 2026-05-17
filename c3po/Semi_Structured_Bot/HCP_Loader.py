import json
import dask.dataframe as dd
import pandas as pd

class HCPFilesLoader:
    BASE_PATH = "./Semi_Structured_Bot/"
    INPUT_FILES_PATH = BASE_PATH + "Input_Files/"
    CONFIG_FILES_PATH = BASE_PATH + "Config_Files/"
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
    def load_tools(cls):
        """Loads all JSON file descriptions from Input_Files/."""
        print(cls.INPUT_FILES_PATH)
        descriptions = {
            "functions_hcp":cls.load_json_file(cls.INPUT_FILES_PATH + "tools_hcp_opinions.json")
        }
        print("Loaded hcp tools ")
        return descriptions

    @classmethod
    def load_instructions(cls):

        """Loads all instruction files from Input_Files/."""
        instructions = {
            "opensearch_hybrid_query_generation_prompt":cls.load_text_file(cls.INPUT_FILES_PATH + "opensearch_query_generation_prompt.txt"),
        }
        print("Loaded all hcp instructions")
        return instructions