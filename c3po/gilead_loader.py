import json
import dask.dataframe as dd
import pandas as pd
import os

class GileadDataLoader:
    def __init__(self, base_folder=None):
        """
        Initialize the loader with a base folder path.
        If no folder is provided, uses the default structure.
        """
        print(f"Initializing GileadDataLoader with base folder: {base_folder}")
        if base_folder:
            self.BASE_PATH = base_folder if base_folder.endswith('/') else base_folder + '/'
            # Extract folder name from path for the key
            self.folder_name = os.path.basename(os.path.normpath(base_folder))
        else:
            self.BASE_PATH = "./Structured_Bot/"
            self.folder_name = "Structured_Bot"
        
        self.INPUT_FILES_PATH = self.BASE_PATH + "Input_Files/"
        self.CONFIG_FILES_PATH = self.BASE_PATH + "Config_Files/"

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

    def load_tools(self):
        """Loads all JSON file descriptions from Input_Files/ dynamically."""
        descriptions = {}
        
        # Check if the input files path exists
        if not os.path.exists(self.INPUT_FILES_PATH):
            print(f"Warning: Input files path '{self.INPUT_FILES_PATH}' does not exist.")
            return descriptions
        
        # Get the list of all files in the folder
        for filename in os.listdir(self.INPUT_FILES_PATH):
            # Check if the file ends with .json
            if filename.endswith('.json'):
                # Dynamically use the file name without the extension as the key
                key = filename.split('.')[0]  # Remove '.json' from the filename to use as the key
                descriptions[key] = self.load_json_file(os.path.join(self.INPUT_FILES_PATH, filename))
        
        print(f"Loaded {len(descriptions)} tools from {self.INPUT_FILES_PATH}")
        return descriptions

    def load_instructions(self):
        """Loads all instruction files from Input_Files/ dynamically."""
        instructions = {}
        
        # Check if the input files path exists
        if not os.path.exists(self.INPUT_FILES_PATH):
            print(f"Warning: Input files path '{self.INPUT_FILES_PATH}' does not exist.")
            return instructions
        
        # Get the list of all files in the folder
        for filename in os.listdir(self.INPUT_FILES_PATH):
            # Check if the file ends with .txt or .py
            if filename.endswith(('.txt', '.py')):
                # Dynamically use the file name without the extension as the key
                key = filename.split('.')[0]  # Remove extension from filename to use as the key
                instructions[key] = self.load_text_file(os.path.join(self.INPUT_FILES_PATH, filename))
        
        print(f"Loaded {len(instructions)} instruction files from {self.INPUT_FILES_PATH}")
        return instructions

    def load_configs(self):
        """Loads all the config files from Config_Files/ dynamically."""
        configurations = {}
        
        # Check if the config files path exists
        if not os.path.exists(self.CONFIG_FILES_PATH):
            print(f"Warning: Config files path '{self.CONFIG_FILES_PATH}' does not exist.")
            return configurations
        
        # Get the list of all files in the folder
        for filename in os.listdir(self.CONFIG_FILES_PATH):
            # Check if the file ends with .json
            if filename.endswith('.json'):
                # Dynamically use the file name without the extension as the key
                key = filename.split('.')[0]  # Remove '.json' from the filename to use as the key
                configurations[key] = self.load_json_file(os.path.join(self.CONFIG_FILES_PATH, filename))
        
        print(f"Loaded {len(configurations)} config files from {self.CONFIG_FILES_PATH}")
        return configurations

    def load_questions(self):
        """Loads all the questions files from Input_Files/ dynamically."""
        questions = {}
        
        # Check if the input files path exists
        if not os.path.exists(self.INPUT_FILES_PATH):
            print(f"Warning: Input files path '{self.INPUT_FILES_PATH}' does not exist.")
            return questions
        
        # Get the list of all files in the folder
        for filename in os.listdir(self.INPUT_FILES_PATH):
            # Check if the file ends with .xlsx
            if filename.endswith('.xlsx'):
                # Dynamically use the file name without the extension as the key
                key = filename.split('.')[0]  # Remove '.xlsx' from the filename to use as the key
                questions[key] = self.load_xl_file(os.path.join(self.INPUT_FILES_PATH, filename))
        
        print(f"Loaded {len(questions)} question files from {self.INPUT_FILES_PATH}")
        return questions

    def load_all_gilead_files(self):
        """Runs all file loading methods and returns nested dictionary structure."""
        print(f"Loading files from base path: {self.BASE_PATH}")
        
        # Create nested structure with folder name as top level
        result = {
            self.folder_name: {
                "tools": self.load_tools(),
                "instructions": self.load_instructions(),
                "questions": self.load_questions(),
                "configs": self.load_configs(),
            }
        }
        
        return result

    # Class method for backward compatibility
    @classmethod
    def load_all_from_folder(cls, folder_path):
        """Class method to load all files from a specific folder."""
        loader = cls(folder_path)
        return loader.load_all_gilead_files()

    @classmethod
    def load_multiple_folders(cls, folder_paths):
        """Load from multiple folders and combine into single nested dictionary."""
        combined_result = {}
        print(f"Loading files from multiple folders: {folder_paths}")
        for folder_path in folder_paths:
            print(f"Processing folder: {folder_path}")
            loader = cls(folder_path)
            folder_data = loader.load_all_gilead_files()
            combined_result.update(folder_data)
        
        return combined_result


# Usage examples:

# Method 1: Single folder
# loader = GileadDataLoader("/path/to/your/folder")
# data = loader.load_all_gilead_files()
# Access like: data["folder_name"]["instructions"]["filename"]

# Method 2: Multiple folders at once
# folder_paths = ["/path/to/folder1", "/path/to/folder2", "/path/to/folder3"]
# data = GileadDataLoader.load_multiple_folders(folder_paths)
# Access like: data["folder1"]["instructions"]["filename"]
#              data["folder2"]["configs"]["config_name"]

# Method 3: Class method for single folder
# data = GileadDataLoader.load_all_from_folder("/path/to/your/folder")

# Example usage:
# loader = GileadDataLoader("./my_custom_folder")
# data = loader.load_all_gilead_files()
# 
# # Access instructions
# instructions = data["my_custom_folder"]["instructions"]["some_instruction_file"]
# 
# # Access tools
# tool = data["my_custom_folder"]["tools"]["some_tool_file"]
# 
# # Access configs
# config = data["my_custom_folder"]["configs"]["some_config_file"]
# 
# # Access questions
# questions = data["my_custom_folder"]["questions"]["some_question_file"]