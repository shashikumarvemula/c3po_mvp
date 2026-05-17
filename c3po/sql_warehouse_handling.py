from dotenv import load_dotenv
load_dotenv()
from databricks import sql
from datetime import datetime
import os
import pandas as pd
from databricks.sql.exc import (
    MaxRetryDurationError,
    RequestError,
    SessionAlreadyClosedError,
)
from chainlit.oauth_providers import providers,get_oauth_provider

class DataWarehouse:
    def __init__(self,host_name,http_path,access_token):
        self.host_name=host_name
        self.http_path=http_path
        self.access_token=access_token
        self.connection=None
        self.cursor=None
        
    def reconnect(self):
        try:
            self.timestamp=datetime.now()
            self.connection=sql.connect(
                server_hostname=self.host_name,
                http_path=self.http_path,
                access_token=self.access_token,
                # auth_type="databricks-oauth",
                _tls_no_verify = True
            )
            self.cursor=self.connection.cursor()
            print("Connection established")
        except Exception as e:
            print(f"Error while reconnecting to session at reconnect method : {str(e)}")
            raise e
        
    def one_hour_check(self):
        total_sec=(datetime.now()-self.timestamp).total_seconds()
        if total_sec<=3600 and total_sec>=3200:
            return True
        return False
    
    def refresh_token(self):
        provider=get_oauth_provider("databricks")
        self.access_token=os.getenv("DATABRICKS_TOKEN")
        return self.access_token

    def get_data_from_wareshouse(self, sql_query):
        try:
            self.access_token = self.refresh_token()
            with sql.connect(server_hostname=self.host_name, http_path=self.http_path, access_token=self.access_token, _tls_no_verify=True) as connection:
                print("connection with")
                with connection.cursor() as cursor:
                    print("cursor with")
                    print("In warehouse executing")
                    cursor.execute(sql_query)
                    
                    # Get column names
                    columns = [elem[0] for elem in cursor.description]
                    
                    # Fetch all results
                    result = pd.DataFrame(cursor.fetchall(), columns=columns)
                    print(result.head())
                    
                    # Check if this is a DSG deck query (has dsg_deck column and json_format_df)
                    if 'dsg_deck' in columns and 'json_format_df' in columns:
                        print("DSG deck data detected - converting JSON to DataFrame")
                        
                        # Get the JSON string from json_format_df column
                        if len(result) > 0:
                            json_data = result.iloc[0]['json_format_df']
                            
                            # Convert JSON string to DataFrame
                            result = pd.read_json(json_data, orient='records')
                            print(f"Converted JSON to DataFrame with {len(result)} rows")
                            print(result.head())
                    
            return result
        
        except RequestError as e:
            print("Here is the error while connecting at RequestError:", e)
            self.refresh_token()
            print("Access token refreshed at RequestError:", self.access_token)
            print("reconnected successfully at request error exception")
            return self.get_data_from_warehouse(sql_query)
        
        except Exception as e:
            print(f"Error in sql warehouse {e}")
            raise e