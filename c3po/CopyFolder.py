import shutil
import os
from pathlib import Path

def copy_folders(source_base, destination_base):
    """
    Copy configs and inputs folders from source to destination
    
    Args:
        source_base (str): Source folder name (e.g., "onc_input_data" or "hiv_input_data")
        destination_base (str): Destination folder name (e.g., "Structured_bot")
    """
    
    folders_to_copy = ["Config_Files", "Input_Files"]
    print(f"copying folders from {source_base} to {destination_base}")
    
    for folder in folders_to_copy:
        source_path = Path(source_base) / folder
        dest_path = Path(destination_base) / folder
        print(f"copying folders from {source_path} to {dest_path}")

        
        if source_path.exists():
            # Remove destination folder if it exists
            if dest_path.exists():
                shutil.rmtree(dest_path)
            
            # Copy entire folder
            shutil.copytree(source_path, dest_path)
            print(f"Copied {source_path} -> {dest_path}")
        else:
            print(f"Source folder {source_path} not found")
    print(f"copying folders from {source_base} to {destination_base} completed")


# Usage examples:

# Copy from onc_input_data to Structured_bot
# copy_folders("working_version", "Structured_bot")

# Copy from hiv_input_data to Structured_bot  
# copy_folders("hiv_input_data", "Structured_bot")