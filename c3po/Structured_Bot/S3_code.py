import boto3
import pytz
import os
from time import time


class LoadingFilesFromS3:
    def __init__(self, bucket_name, folder_name):
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        # self.s3 = boto3.client('s3')
        # Initialize timestamps
        self.prev_s3_timestamp_for_files = {}

    
    def list_s3_subfolders(self,bucket_name, parent_folder):
        """
        List all subfolders within a specific folder in an S3 bucket.
        
        Args:
            bucket_name (str): Name of the S3 bucket
            parent_folder (str): The parent folder path to list subfolders from
            
        Returns:
            list: List of subfolder names
        """
        try:
            # Initialize S3 client
            s3_client = boto3.client('s3',region_name='us-west-2')
            
            # Ensure parent_folder ends with a slash
            if parent_folder and not parent_folder.endswith('/'):
                parent_folder += '/'
                
            # List objects with delimiter to get folder-like structure
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=parent_folder,
                Delimiter='/'
            )
            
            subfolders = []
            
            # Extract common prefixes (folders)
            if 'CommonPrefixes' in response:
                for obj in response['CommonPrefixes']:
                    # Get the subfolder name without the parent path and trailing slash
                    folder_path = obj['Prefix']
                    subfolder_name = folder_path[len(parent_folder):-1]  # Remove parent path and trailing slash
                    if subfolder_name:  # Only add non-empty folder names
                        subfolders.append(subfolder_name)
                        
            # Handle pagination if there are more results
            while response.get('IsTruncated', False):
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=parent_folder,
                    Delimiter='/',
                    ContinuationToken=response['NextContinuationToken']
                )
                
                if 'CommonPrefixes' in response:
                    for obj in response['CommonPrefixes']:
                        folder_path = obj['Prefix']
                        subfolder_name = folder_path[len(parent_folder):-1]
                        if subfolder_name:
                            subfolders.append(subfolder_name)
            
            return subfolders
            
        except Exception as e:
            print(f"Error: {e}")
            return []

    def list_files(self,sub_folder):
        print("Listing files from S3...")
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.folder_name+sub_folder)
        print(f"able to list the files from S3: ")
        print(f"Response: {response}")
        return response.get('Contents', [])

    def convert_to_local_time(self, last_modified_utc):
        """Convert the UTC timestamp to local time."""
        local_tz = pytz.timezone('Asia/Kolkata')
        last_modified_local = last_modified_utc.replace(tzinfo=pytz.UTC).astimezone(local_tz)
        return last_modified_local

    def check_file_modified(self, file_key, last_modified_s3_timestamp):
        """Check if the file was modified by comparing the timestamp."""
        files_modified_at_s3_bucket = False
        if file_key not in self.prev_s3_timestamp_for_files or self.prev_s3_timestamp_for_files[file_key] != last_modified_s3_timestamp:
            print(f"File {file_key} Was Modified")
            self.prev_s3_timestamp_for_files[file_key] = last_modified_s3_timestamp
            files_modified_at_s3_bucket = True
        return files_modified_at_s3_bucket

    def download_and_save_file(self, file_key, last_modified_s3_timestamp, BASE_PATH, INPUT_FILES_PATH, CONFIG_FILES_PATH):
        """Download and save the modified file to local storage."""
        print("inside download and save file")
        file_obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        # print(f"file_obj: {file_obj}")
        file_content = file_obj['Body'].read()
        # print(f"file_content: {file_content}")

        file_name = file_key.split("/")[-1]  # Get the file name from path
        print(f'[S3 Last Modified: {last_modified_s3_timestamp}] Processing file: {file_name}')

        # Determine the appropriate path based on the file extension
        if "configs" in file_name:
            save_path = CONFIG_FILES_PATH
        elif file_name.endswith(".py") or file_name.endswith(".pptx") :
            if BASE_PATH.endswith("/"):
                BASE_PATH = BASE_PATH[:-1]
            save_path = BASE_PATH
        else:
            save_path = INPUT_FILES_PATH

        # Ensure the directory exists
        os.makedirs(save_path, exist_ok=True)

        # Save the file locally
        with open(f"{save_path}/{file_name}", 'wb') as local_file:
            local_file.write(file_content)
        
        print(f'[S3 Last Modified: {last_modified_s3_timestamp}] File saved: {save_path}/{file_name}')

    def load_files_from_s3(self):
        """Main method to load files from S3."""
        files_modified_at_s3_bucket_global = False
        session = boto3.Session()
        self.s3 = session.client('s3',region_name='us-west-2')

        print("Loading the data...")

        start_time = time()
        
        folders = self.list_s3_subfolders(self.bucket_name, self.folder_name)
        # List the files from S3
        print(f"Subfolders in {self.folder_name}:",folders)
        for sub_folder in folders:
            sub_folder+='/'
            files = self.list_files(sub_folder)
            print(f"Files in folder {sub_folder}:")
            print(files)
            if not files:
                print('No files found in the folder.')
                continue

            if not files:
                print('No files found in the folder.')
                return

            for obj in files:

                file_key = obj['Key']
                if file_key == self.folder_name+sub_folder:  # Skip the folder itself
                    continue

                last_modified = obj['LastModified']  # This is in UTC
                last_modified_local = self.convert_to_local_time(last_modified)

                # Format it for display
                last_modified_s3_timestamp = last_modified_local.strftime('%Y-%m-%d %I:%M:%S %p %Z')
                print(f'[S3 Last Modified: {last_modified_s3_timestamp}] Reading file: {file_key}')

                # Check if the file was modified
                files_modified_at_s3_bucket = self.check_file_modified(file_key, last_modified_s3_timestamp)
                BASE_PATH = f"./{sub_folder}"
                INPUT_FILES_PATH = BASE_PATH + "Input_Files"
                CONFIG_FILES_PATH = BASE_PATH + "Config_Files"
                if files_modified_at_s3_bucket:
                    files_modified_at_s3_bucket_global = True
                    self.download_and_save_file(file_key, last_modified_s3_timestamp, BASE_PATH, INPUT_FILES_PATH, CONFIG_FILES_PATH)
                else:
                    print(f"File is not Modified at S3: {file_key}")

                print(f'Name of the File is - {file_key}')

            end_time = time()
            print(f'Time Taken for Loading Files: {end_time - start_time}')
        return files_modified_at_s3_bucket_global

# bucket_name = 'gilead-c3po-input-data'  
# folder_name = 'c3po-3-jan/'  

# loader = LoadingFilesFromS3(bucket_name, folder_name)

# loader.load_files_from_s3()
