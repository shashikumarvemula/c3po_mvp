import chainlit as cl
from fastapi import FastAPI
import boto3
import json
import uvicorn
import time
import json
import ast
import os
import requests
import io
import tiktoken
import langchain
from langchain_experimental.tools.python.tool import PythonAstREPLTool
from chainlit.input_widget import Select
import pandas as pd
import dask.dataframe as dd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from zoneinfo import ZoneInfo
from typing import Optional
import re
from datetime import datetime, timedelta
from openai import OpenAI,AsyncOpenAI
import asyncio
from dotenv import load_dotenv
load_dotenv()

import threading
import time
import psutil
import logging
import builtins

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def log_memory_usage(interval=60):
    process = psutil.Process()
    while True:
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / 1024 / 1024
        logging.info(f"Memory Usage (RSS): {rss_mb:.2f} MB")
        time.sleep(interval)

# Store the original print function
original_print = builtins.print

def print_with_memory(*args, **kwargs):
    """Override print to show memory usage first, then the actual print content"""
    # Get current memory usage
    process = psutil.Process()
    mem_info = process.memory_info()
    rss_mb = mem_info.rss / 1024 / 1024
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Print memory first
    original_print(*args,f"[Memory: {rss_mb:.2f} MB] at time : [{timestamp}]", **kwargs)
    
    # Then print the original content
    # original_print(*args, **kwargs)

# Override the built-in print function
builtins.print = print_with_memory

# Start background memory logger
memory_thread = threading.Thread(target=log_memory_usage, daemon=True)
memory_thread.start()


import copy
from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError
import asyncio
from aiobotocore.session import get_session
from chainlit import logger
import logging
logger.getChild("DynamoDB").setLevel(logging.DEBUG)
import chainlit.data as cl_data
from chainlit.data.dynamodb import DynamoDBDataLayer
from chainlit.data.storage_clients.s3 import S3StorageClient
from traceloop.sdk import Traceloop
from traceloop.sdk.decorators import workflow, task
import matplotlib
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
# Metric imports
from opentelemetry.metrics import get_meter
from opentelemetry import metrics as metrics
from Structured_Bot.S3_code import LoadingFilesFromS3

bucket_name = os.environ["bucket_name"]
secret_name = os.environ["secret_name"]
folder_name = os.environ["folder_name"]
model_id= os.environ["model_id"]
dynamo_bucket_name = os.environ["dynamo_bucket_name"]
loader = LoadingFilesFromS3(bucket_name, folder_name)
print("loading data")
loader.load_files_from_s3()


from Structured_Bot.gilead_loader import GileadDataLoader
from Structured_Bot.ui_utils import UI_Utils
from Structured_Bot.llm_requests_new  import LLM
from Structured_Bot.helper import FileNameExtractor 
# from Structured_Bot.repl_tool_execution import PythonExecutionManager


from Structured_Bot.relevant_source_instructions import get_relevant_source_instructions_processor


from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from chainlit.element import ElementDict
from chainlit.logger import logger
from chainlit.types import Feedback
from dataclasses import asdict
from botocore.exceptions import ClientError
import asyncio
from aiobotocore.session import get_session

from databricks_provider import DatabricksProvider
from inject_custom_auth import add_custom_oauth_provider

session = get_session()

BOT_TYPE_STRUCTURED = "Structured"
session = get_session()

#tools exectuion imports
from ToolCalls import ToolCallsExecutor
tool_executor = ToolCallsExecutor()
from Files_Images_Handling import ElementsHandling
files_images_handler=ElementsHandling()
tool_params = {"arguments":{}, "dataware_house":{}, "user_message":{}, "result":{}}
from chainlit.oauth_providers import providers,get_oauth_provider



# # Gilead Credentials
# bucket_name = 'gilead-edp-commercial-dev-us-west-2-us-onc-iidd-genai'  

# secret_name = "us-onc-iidd-genai/databricks-token"



# Setuserv Credentials
# bucket_name = 'gilead-c3po-input-data'  # bucket name for prompt
# # bucket_name = 'query-resultsbucket-1'  # bucket name for Dynamodb
# folder_name = 'c3po-3-jan/'

# file_path = 'c3po-logs-TEST-TEMP/2025-03-20_22-47-10.log' 
# secret_name = 'us-onc-iidd-genai/databricks-token'


def get_secret(secret_name, **kwargs):

        """
        Retrieve individual secrets from AWS Secrets Manager using the get_secret_value API.
        This function assumes the stack mentioned in the source code has been successfully deployed.
        This stack includes 7 secrets, all of which have names beginning with "mySecret".

        :param secret_name: The name of the secret fetched.
        :type secret_name: str
        """
        session = boto3.Session()
        client = session.client("secretsmanager", region_name="us-west-2")
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            if len(kwargs.keys()) > 0:
                key = kwargs['key']
                print(key)
                secret = json.loads(str(get_secret_value_response["SecretString"]))[key]
            else:
                 secret = get_secret_value_response["SecretString"]
            return secret

        except client.exceptions.ResourceNotFoundException:
            msg = f"The requested secret {secret_name} was not found."
            return msg
        except Exception as e:
            raise e

os.environ["DATABRICKS_ENDPOINT_URL"] = get_secret(secret_name,key="DATABRICKS_ENDPOINT_URL")
os.environ["DATABRICKS_TOKEN"] = get_secret(secret_name,key="databricks-token")
os.environ["CHAINLIT_AUTH_SECRET"] = get_secret(secret_name,key="CHAINLIT_AUTH_SECRET")
os.environ["TRACELOOP_BASE_URL"] = get_secret(secret_name,key="TRACELOOP_BASE_URL")
os.environ["TRACELOOP_HEADERS"]=get_secret(secret_name,key="TRACELOOP_HEADERS")
os.environ["DATABRICKS_SERVER_HOSTNAME"]=get_secret(secret_name,key="DATABRICKS_SERVER_HOSTNAME")
os.environ["DATABRICKS_HTTP_PATH"]=get_secret(secret_name,key="DATABRICKS_HTTP_PATH")
os.environ["OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE"]="delta"
os.environ["OAUTH_OKTA_CLIENT_ID"]=get_secret(secret_name,key="OAUTH_OKTA_CLIENT_ID")
os.environ["OAUTH_OKTA_CLIENT_SECRET"]=get_secret(secret_name,key="OAUTH_OKTA_CLIENT_SECRET")

os.environ["DATABRICKS_CLIENT_ID"]=get_secret(secret_name,key="DATABRICKS_CLIENT_ID")
os.environ["DATABRICKS_CLIENT_SECRET"]=get_secret(secret_name,key="DATABRICKS_CLIENT_SECRET")

os.environ["OAUTH_OKTA_AUTHORIZATION_SERVER_ID"]="false"
provider=get_oauth_provider("okta")
provider.domain=f"https://{os.environ.get('OAUTH_OKTA_DOMAIN', '').rstrip('/')}"
provider.authorize_url=f"{provider.domain}/oauth2{provider.get_authorization_server_path()}/v1/authorize"
provider.client_id=os.environ.get("OAUTH_OKTA_CLIENT_ID")
provider.client_secret=os.environ.get("OAUTH_OKTA_CLIENT_SECRET")

# import chainlit as cl
# import os

# # Get credentials from environment or secrets manager
# client_id = os.environ.get("OAUTH_OKTA_CLIENT_ID")
# Get client_secret from your secrets manager

# Configure the Okta OAuth provider

# Metrics
# Obtain a meter
# Defining custom metrics
Traceloop.init(app_name="C3PO_Testing",should_enrich_metrics=True)
#Obtain a meter
meter = get_meter(__name__)
total_tokens_metric = meter.create_counter(name="Total_Tokens", description="Counter for LLM token usage", unit="1")
total_tokens_cost = meter.create_counter( name="Total_Tokens_Usage_Cost",description="Counter for LLM token usage cost",unit="1")
latency_metric=meter.create_histogram(name="Latency", description="Latency of LLM's",unit="1")
tokens_per_conv=meter.create_counter(name="Tokens_per_conversation",description="Tokens per conversation",unit="1")
response_time_metric=meter.create_counter(name="Response_Time",description="Response time",unit="1")

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from chainlit.element import ElementDict
from chainlit.logger import logger
from chainlit.types import Feedback
from dataclasses import asdict
model_id="claude_3-5_sonnet"

# import boto3
from botocore.exceptions import ClientError
import asyncio
from aiobotocore.session import get_session
session = get_session()

# bucket_name = 'gilead-edp-commercial-dev-us-west-2-us-onc-iidd-genai'  
# folder_name = 'c3po-30-jan/' 
# folder_name = 'c3po-3-jan/'
# session = boto3.Session()


_logger = logger.getChild("DynamoDB")
_logger.setLevel(logging.WARNING)


client = AsyncOpenAI(
        api_key=os.environ['DATABRICKS_TOKEN'],
        base_url=os.environ["DATABRICKS_ENDPOINT_URL"]
    )


# import boto3

session = get_session()

#bucket_name = 'gilead-edp-commercial-dev-us-west-2-us-onc-iidd-genai'  
# folder_name = 'c3po-30-jan/' 

request_llm =LLM()

gilead_loader = GileadDataLoader()

# execution_manager = PythonExecutionManager(gilead_loader)

instructions = gilead_loader.load_instructions()
tools = gilead_loader.load_tools()
tools_structured = tools.get("llm_tool_call_functions")

# repl_tool = PythonExecutionManager(gilead_loader)
print("after loading all instructions")
from Structured_Bot.handle_clickable_questions import ClickableQuestionHandler
from Structured_Bot.sending_req_to_llm import RequestLLM
from Structured_Bot.chatbot_handler import ChatbotHandler 
# execution_manager = PythonExecutionManager(gilead_loader)
chatbot_handler = ChatbotHandler()

sending_request_to_llm = RequestLLM()


from sql_warehouse_handling import DataWarehouse
http_path =get_secret(secret_name,key="DATABRICKS_HTTP_PATH")
host_name=get_secret(secret_name,key="DATABRICKS_SERVER_HOSTNAME")
# sql_warehouse()
dataware_house=None
databricks_auth_token=None
global result

# def refresh_data(result):
#     if result:
#         print("Running loading_code_gilead in REPL...")
#         gilead_loader.load_sales_and_claims_data()
#         asyncio.run(execution_manager.run_loading_code())
#         print("Loading code executed successfully.")
#     else:
#         print(f"No Changes Made to refresh the Data")


# result = loader.load_files_from_s3()
# refresh_data(result)

MAX_ITER = 15

load_dotenv()

# print("Before authorization")
add_custom_oauth_provider("databricks",DatabricksProvider())
@cl.oauth_callback
def oauth_callback(
    provider_id: str,
    token: str,
    raw_user_data: Dict[str, str],
    default_user: cl.User,
) -> Optional[cl.User]:
    print("In authorization")
    print(f"OAuth callback triggered for provider: {provider_id}")
    print(f"User data: {raw_user_data}")
    if provider_id=="databricks":
        global databricks_auth_token
        databricks_auth_token=token
        user=cl.User(
            identifier=default_user.identifier,
            metadata={
                **default_user.metadata,
                "access_token":token,
                "provider":provider_id,
            }
        )

    # # result = loader.load_files_from_s3()
    # print("after refresh of data in auth")
    # if result:
    #     print("Files Modified at S3")
    # else:
    #     print("Files Was not Modified at S3")

    return default_user

print("Before authorization")
@cl.password_auth_callback
def auth_callback(username: str, password: str):# -> Optional[cl.AppUser]:
  # Fetch the user matching username and compare the password with the value stored in the database
  usernames_passwords_lst = [('husna.siddiqa@setuserv.com', 'setuserv123'), ('kalyankumar.marella@setuserv.com', 'setuserv123'), ('sanga', 'setuserv123'), ('sanjeev.kayath@gilead.com', 'gilead123'), ('gaurav.bhatnagar@gilead.com', 'gilead123'), ('sadhna.thakur5@gilead.com', 'gilead123'), ('Badari.Ganti@gilead.com', 'gilead123'), ('karthik.jodu@setuserv.com', 'setuserv123'), ('shashikumar.vemula@setuserv.com','setuserv123'),('samvidha.reddy@setuserv.com', 'setuserv123'), ('parashuram.reddy@setuserv.com','setuserv123'), ('Sumit.Singh70@gilead.com', 'gilead123'), ('sanga@setuserv.com', 'setuserv123'), ('srikanth.reddy@setuserv.com', 'setuserv123'), ('kristi.pedersen@gilead.com', 'gilead123'), ('SangaReddy.Peerreddy@gilead.com', 'gilead123'), ('diego.dimes@gilead.com', 'gilead123'), ('samvidha.reddy@setuserv.com', 'setuserv123'),('sayantan.dasgupta@arvinas.com','arvinas123'),('yshankar@prescriptiveinsights.com','prescriptive123'),('suresh.divakar@prescriptiveinsights.com','prescriptive123'),('avani.patlolla@setuserv.com','setuserv123'),('chandana.rajarapu@setuserv.com','setuserv123'),('vikram@multiplierai.com','multiplierai123'),('guest@setuserv.com','setuserv123'),('nalini.purkayastha@gilead.com','gilead123'),('shomita.mandal@gilead.com','gilead123'),('pete.bielecki@gilead.com','gilead123'),('carolynn.chang@gilead.com','gilead123'),('nadia.cole@gilead.com','gilead123')]
  new_lst = []
  for tup in usernames_passwords_lst:
      print("In authorization")
      new_lst.append((tup[0].lower(), tup[1].lower()))
  if (username.lower(), password.lower()) in new_lst:
    #cl.user_session.set("username", username)
    result = loader.load_files_from_s3()
    print("after refresh of data in auth")
    if result:
        print("Files Modified at S3")
    else:
        print("Files Was not Modified at S3")
    # refresh_data(result)
    return cl.User(identifier=username, metadata={"role": "admin", "provider": "credentials"})
  else:
    return None




# # Run the async function in an event loop
# asyncio.run(execution_manager.run_loading_code())



@cl.step

@cl.on_chat_start
async def start():
    await chatbot_handler.start_chat()

    print("inside start_Chart/............")
    user=cl.user_session.get("user")
    if not user or not user.metadata.get("access_token"):
        redirect_url=providers[-1].redirect_uri
        domain=redirect_url[:redirect_url.find("/auth")]
        url=f"{domain}/auth/oauth/databricks"
        linked_text = f"❌ You're not authenticated. Please login [here]({url})."
        await cl.Message(content=linked_text).send()

session_boto3=boto3.Session()
s3_client = boto3.client('s3',region_name="us-west-2")
class CustomDynamoDBDataLayer(DynamoDBDataLayer):

    def generate_presigned_url(self,bucket_name,object_key,expiration=3600):
        try:
            url = s3_client.generate_presigned_url('get_object',
                Params={'Bucket': dynamo_bucket_name, 'Key': object_key},
                ExpiresIn=expiration)
            return url
        except Exception as e:
            print(f"Error generating pre-signed URL: {e}")
            return None


    def _deserialize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # print("checking signed url:",item)
        val=0
        url_value=""
        sample={}
        for key,value in item.items():
            if key=="url":
                print("Key Value : ",value)
                index=value['S'].find("s3.amazonaws.com")
                index=index+len("s3.amazonaws.com")
                object_key=value['S'][index+1:]
                first_index=value['S'].find("https://")
                first_index=first_index+len("https://")
                last_index=value['S'].find("s3.amazonaws.com")
                bucket_name=value['S'][first_index:last_index-1]
                value['S']=self.generate_presigned_url(dynamo_bucket_name,object_key)
                #print("After Modified : Value ",value)
            #sample[key]=self._type_deserializer.deserialize(value)
            sample={ key: self._type_deserializer.deserialize(value) }
            #print(f"sample_{val} : {sample}")
            val+=1
        return {
            key: self._type_deserializer.deserialize(value)
            for key, value in item.items()
        } 
    
    async def upsert_feedback(self, feedback: Feedback) -> str:
        _logger.info(
            "DynamoDB: upsert_feedback thread=%s step=%s value=%s",
            feedback.threadId,
            feedback.forId,
            feedback.value,
        )

        if not feedback.forId:
            raise ValueError(
                "DynamoDB data layer expects value for feedback.threadId got None"
            )

        feedback.id = f"THREAD#{feedback.threadId}::STEP#{feedback.forId}"
        serialized_feedback = self._type_serializer.serialize(asdict(feedback))

        self.client.update_item(
            TableName=self.table_name,
            Key={
                "PK": {"S": f"THREAD#{feedback.threadId}"},
                "SK": {"S": f"STEP#{feedback.forId}"},
            },
            UpdateExpression="SET #feedback = :feedback",
            ExpressionAttributeNames={"#feedback": "feedback"},
            ExpressionAttributeValues={":feedback": serialized_feedback},
        )
        #print("Feedback inserted: ",feedback)
        return feedback.id
    async def get_thread(self, thread_id: str) -> "Optional[ThreadDict]":
        print("DynamoDB: get_thread thread=%s", thread_id)
        # Get all thread records
        thread_items: List[Any] = []
        cursor: Dict[str, Any] = {}
        while True:
            response = self.client.query(
                TableName=self.table_name,
                KeyConditionExpression="#pk = :pk",
                ExpressionAttributeNames={"#pk": "PK"},
                ExpressionAttributeValues={":pk": {"S": f"THREAD#{thread_id}"}},
                **cursor,
            )
            deserialized_items = map(self._deserialize_item, response["Items"])
            thread_items.extend(deserialized_items)
            if "LastEvaluatedKey" not in response:
                break
            cursor["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        if len(thread_items) == 0:
            return None
        # process accordingly
        thread_dict: Optional[ThreadDict] = None
        steps = []
        elements = []
        for item in thread_items:
            if item["SK"] == "THREAD":
                thread_dict = item
            elif item["SK"].startswith("ELEMENT"):
                elements.append(item)
            elif item["SK"].startswith("STEP"):
                if "feedback" in item:  # Decimal is not json serializable
                    item["feedback"]["value"] = int(item["feedback"]["value"])
                steps.append(item)
        if not thread_dict:
            if len(thread_items) > 0:
                async def get_thread(self, thread_id: str) -> "Optional[ThreadDict]":
                    print("DynamoDB: get_thread thread=%s", thread_id)
        # Get all thread records
        thread_items: List[Any] = []
        cursor: Dict[str, Any] = {}
        while True:
            response = self.client.query(
                TableName=self.table_name,
                KeyConditionExpression="#pk = :pk",
                ExpressionAttributeNames={"#pk": "PK"},
                ExpressionAttributeValues={":pk": {"S": f"THREAD#{thread_id}"}},
                **cursor,
            )
            deserialized_items = map(self._deserialize_item, response["Items"])
            thread_items.extend(deserialized_items)
            if "LastEvaluatedKey" not in response:
                break
            cursor["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        if len(thread_items) == 0:
            return None
        # process accordingly
        thread_dict: Optional[ThreadDict] = None
        steps = []
        elements = []
        for item in thread_items:
            if item["SK"] == "THREAD":
                thread_dict = item
            elif item["SK"].startswith("ELEMENT"):
                elements.append(item)
            elif item["SK"].startswith("STEP"):
                if "feedback" in item:  # Decimal is not json serializable
                    item["feedback"]["value"] = int(item["feedback"]["value"])
                steps.append(item)
        if not thread_dict:
            if len(thread_items) > 0:
                print(
                    "DynamoDB: found orphaned items for thread=%s", thread_id)
            return None
        steps.sort(key=lambda i: i["createdAt"])
        thread_dict.update(
            {
                "steps": steps,
                "elements": elements,
            }
        )
        return thread_dict
def create_dynamodb_client():
    try:
        # Create DynamoDB resource
        dynamodb = session_boto3.resource('dynamodb', region_name='us-west-2')
        
        # Verify table exists
        table_name="us-onc-iidd-genai-feedback"
        table = dynamodb.Table(table_name)
        table.load()  # This will raise an error if table doesn't exist
        
        print("DynamoDB table verified successfully")
        return table
    except ClientError as e:
        print(f"DynamoDB Table Error: {e}")
        # You might want to create the table here or handle the error appropriately
        raise

def create_storage_client():
    try:
        
        #session=boto3.Session()
        s3_client = session_boto3.client('s3',region_name='us-west-2')
        
        return S3StorageClient(
            bucket=dynamo_bucket_name,
            region_name="us-west-2"
        )
    except Exception as e:
        print(f"AWS Credentials Error: {e}")
        raise

def setup_storage():
    try:
        # Create storage and verify S3 and DynamoDB access
        storage_client = create_storage_client()
        create_dynamodb_client()
        return storage_client
    except Exception as e:
        print(f"Storage Setup Error: {e}")
        raise


storage_client=setup_storage()

@cl.data_layer
def data_layer():
    return CustomDynamoDBDataLayer(
        table_name="us-onc-iidd-genai-feedback", 
        storage_provider=storage_client,
        user_thread_limit=50 
    )

data_layer_instance = CustomDynamoDBDataLayer(table_name="us-onc-iidd-genai-feedback", storage_provider=storage_client)
def extract_message_history(thread_data):
    message_history = []
    # Process each step
    sublist=thread_data.get('steps')
    print("sublist check if has code or not",sublist)
    for step in thread_data.get('steps', []):
        step_type = step.get('type', '')
        content = step.get('output', '') 
        if step_type == 'user_message':
            message_history.append({
                'content': content,
                'role': 'user'
            })
        elif step_type == 'llm' and content != '' and content !='Data Retrieved Successfully':
            message_history.append({
                'content': content,
                'role': 'assistant',
                'tool_calls': step.get('tool_calls', [])
            })
        elif step_type == 'run' and content != '' and content !='Data Retrieved Successfully':
            message_history.append({
                'content': content,
                'role': 'tool',
                'tool_call_id': step.get('id', '')
            })
    
    return message_history


@cl.on_settings_update
async def update_settings(settings):
    await chatbot_handler.setup_agent(settings)
    
async def stream_on_ui(text):
    """
    Streams a token to the UI.
    """
    message = cl.Message(content="") 
    await message.send()
    for token in text.split():
        await message.stream_token(token + " ")
        await asyncio.sleep(0.1)  # Simulate a delay for streaming
    await message.update()

@cl.on_message
@workflow(name="In conversation")
async def run_conversation(message: cl.Message):
    """
    Handles the user's input message, revises the query if necessary,
    and processes it based on the active chatbot's context (e.g., Gilead).
    """
    global total_tokens_conv
    global input_tokens_conv
    global  output_tokens_conv
    total_tokens_conv=0
    input_tokens_conv=0
    output_tokens_conv=0
    global latency
    latency=0
    start_time = time.time()
    email = cl.user_session.get("user").identifier

    #Extract the user's query from the message
    query = message.content
    global dataware_house,databricks_auth_token
    if dataware_house is None:
        print("SQL Wareshouse Initialization")
        dataware_house=DataWarehouse(host_name,http_path,databricks_auth_token)
        print("SQL Wareshouse Initialized")
    
    tool_params["dataware_house"]=dataware_house

    # Retrieve the message history from the user session
    message_history = cl.user_session.get("message_history")
    print("message_history!!!!!!!!!!!!!!",len(message_history),  message_history)

    # Retrieve the active chatbot name from the session
    chatbot_name = cl.user_session.get("chatbot")
    print("chatbot_name!!!!!!!!!!!", chatbot_name)

    # Check if the active chatbot is "Structured"
    if chatbot_name == BOT_TYPE_STRUCTURED:
        print("in chatbot_name !!!")
        
        # If there is a message history, revise the query
        if len(message_history) > 1:
            # revised_query = await sending_request_to_llm.get_revised_query(revised_query_prompt, message_history, query)
            revised_query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.get_revised_query(instructions.get("revised_query_instructions"), message_history, query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency)
        else:
            revised_query = query # No revision needed for the first query
        print("The query is!!!!!!!!!!!", query)
        print("revised_query!!!!!!!!!!!!!", revised_query)

        await stream_on_ui("Revised query : "+revised_query)

        print("This is the revised query",revised_query)
        config = GileadDataLoader.load_configs()
        descriptions = GileadDataLoader.load_tools()
        if len(config["configs"]["list_of_data_sources"])==1:
            data_source = config["configs"]["list_of_data_sources"][0]
            column_descriptions = data_source + "_Column_Descriptions"
            data_source_dict={
                "source": data_source,
                "fields": list(descriptions[column_descriptions].keys())
            }
        else:
            # data_source_dict = await sending_request_to_llm.get_data_source_and_fields(revised_query)
            data_source_dict,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.get_data_source_and_fields(revised_query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency)
        # Determine the data source and fields using Bedrock Claude

        data_source, fields, gilead_prompt, queries_code = await get_relevant_source_instructions_processor(data_source_dict)
        print("Extracted Fields:", fields)

        print("data source for deck automation- ",data_source)
        if data_source != "refresh_deck":
            print("Extracted Data Source:", data_source)
            print("Extracted filed - ", fields)
            text = " Data Sources :- [ " + ", ".join(data_source) + " ]" if isinstance(data_source, list) else data_source 
            text += " Fields :- [ " + ", ".join(fields) + " ]" if isinstance(fields, list) else fields
            await stream_on_ui("Using : "+text )
        
        # Process the revised query with the appropriate data source and prompt
            await handle_user_query(revised_query, data_source, fields=fields, gilead_prompt=gilead_prompt,queries_code=queries_code,email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)
    else:   
        await handle_user_query(query,data_source='', fields=fields, gilead_prompt='', queries_code=queries_code,email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency )

    end_time=time.time()
    response_time=end_time-start_time
    print("response time",response_time)
    response_time_metric.add(response_time)
# relevent_business_rules = await sending_request_to_llm.get_relevent_business_rules(gilead_prompt, user_message_copy)
@task(name="handling user query")
async def handle_user_query(user_message, data_source='', fields=[],  gilead_prompt='',email='', queries_code='',total_tokens_conv=0,input_tokens_conv=0,output_tokens_conv=0,latency=0):
    email = cl.user_session.get("user").identifier
    chatbot_name = cl.user_session.get("chatbot")
    user_message_copy = user_message
    tool_params["user_message"] = user_message
    message_history = cl.user_session.get("message_history")

    if chatbot_name == BOT_TYPE_STRUCTURED:
        if "business" in user_message.lower():
            user_message += ''' To calculate Source of Business Shares:
            - Use occurance(frequency) of each drug/ total occurances(frequency) of each drug. '''
        # user_message = user_message + '''Common Procedures for claims data:
        # - Use year_month to extract year/quarter for using in yearly/ Quartelry analysis'''
        # user_message = user_message + " If the answer is already present in the query, just give out that answer instead of calculating again. If you plot a chart, Save the table behind the chart into a CSV/XLSX file"
        user_message = user_message + " If the answer is an information provided rather than a question i.e already answer is present in the query, just give out that answer instead of calculating again. If indication is mentioned, filter for the indication in all cases. If you plot a chart, Save the table behind the chart into a CSV/XLSX file. Please note you will not have access to existing CSV/PPT files to answer for follow up questions. In that case you need to calculate again"

        print("type:  ", type(cl.user_session))

        await stream_on_ui("Fetching the relevent business rules and examples for the query...")

        chatbot_name = cl.user_session.get("chatbot")
        message_history = cl.user_session.get("message_history")
        print("gilead_prompt structure:", gilead_prompt)
        # relevent_business_rules = await sending_request_to_llm.get_relevent_business_rules(gilead_prompt, user_message_copy,fields)
        relevent_business_rules ,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency= await sending_request_to_llm.get_relevent_business_rules(gilead_prompt, user_message_copy,fields,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency)
        print("the relevant business rules picked are: ",relevent_business_rules)

        relevant_examples,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.pick_relevant_example_queries(user_message_copy, queries_code,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency)
        # relevant_examples = await sending_request_to_llm.pick_relevant_example_queries(user_message_copy, queries_code)
        print("The relevant examples are: ",relevant_examples)
        await stream_on_ui("Fetched the relevent instructions and examples for the query... ")
        await stream_on_ui("Now Proceeding with the next steps...")

        system_prompt = instructions.get("get_analysis_plan_nd_code").format(fields = fields, code_instructions = instructions.get("sql_code_generation_instructions"), business_rules = relevent_business_rules, relevant_examples=relevant_examples)
        print('total system prompt in messages history for the question :',system_prompt)

        # gilead_prompt += 'Foremost step is to tell which data source you are'
        message_history[0] = {"role": "system", "content": system_prompt}
    # print("message_history at start: ", message_history, "histoory type!!!!!!", type(message_history))

    for message in message_history:
        if message['role'] == 'tool':
            message['content'] = ""

    print("after removing function content: ", message_history)

    if len(message_history) > 15:
        del message_history[5:len(message_history) - 9]

    # message_history.append({"role": "user", "content": user_message})
    message_history.append({"role": "user", "content": user_message_copy})
    current_history = message_history[:1] + [{"role": "user", "content": user_message_copy}]
    cur_iter = 0
    print('initial current history : 1',current_history,'\n message_history initial : ',message_history)
    saved_files_lst = []
    sql_code=None
    python_code=None
    function_response=None

    while cur_iter < MAX_ITER:

        # OpenAI call
        claude_message = {"role": "", "content": ""}
        function_ui_message = None
        content_ui_message = cl.Message(content="")

        async with cl.Step(name = "C3PO", type = "llm",default_open=True) as content_ui_message:#disable_feedback=False,root = True
            async with cl.Step(name="Code" if not sql_code else "Code", type="llm", show_input= False, language = "python",default_open=False) as function_ui_message:#root=False, 
                active_bot = chatbot_name
                if active_bot == BOT_TYPE_STRUCTURED:
                    active_bot_functions = tools_structured

                print(f"current_history of {cur_iter}: ",current_history)
                start_time=time.time()
                print("active_bot_functions",active_bot_functions)

                response = await request_llm.send_request_streaming(current_history,tools=active_bot_functions)

                end_time=time.time()
                latency+=(end_time-start_time) 

                # print("RESPONSE : ",response)
                print(response)
                async for stream_resp in response:
                    # print("stream_response = ",stream_resp.choices[0].finish_reason)
                    new_delta = stream_resp.choices[0].delta
                    # print("new_delta=",new_delta)
                    claude_message, content_ui_message, function_ui_message, stream_resp = await UI_Utils.process_new_delta(
                        new_delta, claude_message, content_ui_message, function_ui_message,stream_resp,True)
                    if(stream_resp.usage.prompt_tokens!=None and stream_resp.usage.completion_tokens!=None):
                            prompt_tokens = stream_resp.usage.prompt_tokens
                            completion_tokens = stream_resp.usage.completion_tokens
                            total_tokens = prompt_tokens + completion_tokens
                            input_tokens_conv+=prompt_tokens 
                            output_tokens_conv+=completion_tokens
                            total_tokens_conv+=total_tokens
                            print("first call conv",total_tokens)
                            print("conv input",input_tokens_conv,"conv output", output_tokens_conv,"total conv before code check",total_tokens_conv)
                    #print(chat_completion.choices[0].message.content)
                
                # print("message_history before appending claude message: ", message_history)
                # print("claude_message: ", claude_message)
                message_history.append(claude_message)
                current_history.append(claude_message)

                print(stream_resp.choices[0])
                if stream_resp.choices[0].finish_reason == "stop":
                    break

                if stream_resp.choices[0].finish_reason != "tool_calls" and stream_resp.choices[0].finish_reason != "stop":
                    raise ValueError(stream_resp.choices[0].finish_reason)
                # Safely extract function call details (if they exist)

                if stream_resp.choices[0].finish_reason == "tool_calls":
                    function_name = claude_message.get("tool_calls", [{}])[0].get("function", {}).get("name", "")
                    tool_call_id = claude_message.get("tool_calls", {})[0].get("id", "")
                    argument_str = claude_message.get("tool_calls", [{}])[0].get("function", {}).get("arguments", "")
                    arguments = ast.literal_eval(argument_str)
                    tool_params["arguments"] = arguments

            if function_name == "calculate_date_ranges"  :
                python_ui_message = function_name + argument_str
            else:
                python_ui_message = arguments.get("query")

            print("the python_ui_message is", python_ui_message)
            print("the function_ui_message is", function_ui_message)
                            
            tool = tool_executor.get_tool(function_name)
            function_ui_message.output, function_response, result = await tool.run(tool_params)

            if result is not None:
                tool_params["result"] = result


            if function_ui_message.output is None:
                function_ui_message.output = python_ui_message
            else:
                function_ui_message.output = function_ui_message.output + python_ui_message
            await function_ui_message.update()
            await cl.sleep(0.1)

            
            message_history.append(
                {
                    "role": "tool",
                    "content": function_response,
                    'tool_call_id': tool_call_id,
                }
            )
            current_history.append(
                {
                    "role": "tool",
                    "content": function_response,
                    'tool_call_id': tool_call_id,
                }
            )

            if function_name == 'generate_sql_code' or function_name=="python_after_sql": #or function_name_sql=="generate_sql_code" or function_name_sql=="python_after_sql":
                async with cl.Step(name="Code Execution Output" if sql_code and not python_code else "Code Execution Output", type="llm",show_input= False, language = "python") as python_code_exc_output:# root=False, 
                    print("in output execution !!!!!!!")
                    python_code_exc_output.output = function_response
                    print("after python_code_exc_output !!!!!!")
                    # await python_code_exc_output.update() 
                    await cl.sleep(0.1)
                python_code=ast.literal_eval(argument_str).get("query","")
                print("iam in my files",python_code)
                images_files_list,excel_files_list,ppt_files_list=files_images_handler.get_files_images(python_code,saved_files_lst)



            if function_name=="python_after_sql" or function_name=="generate_sql_code" and len(images_files_list)>0:
                print("images_files_list: ", images_files_list)
                await files_images_handler.saving_files_to_ui(images_files_list)
        
            else:
                print("No images created for this question....")

            if function_name == 'python_after_sql' or function_name == 'generate_sql_code' and len(excel_files_list)>0:
                print("excel_files_list: ", excel_files_list)
                await files_images_handler.excel_file_handler(excel_files_list)



    output_files = copy.deepcopy(saved_files_lst)
    insights_response, content_ui_message,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.insights_module(
    user_message_copy, output_files,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency
    )


    print("\n🔍 **Insights Response:**")
    print(insights_response["content"])


    if content_ui_message:
        content_ui_message.remove()
    for filename in saved_files_lst:
        if os.path.exists(filename):
            try:
                os.remove(filename)
                print(f"{filename} has been deleted.")
            except Exception as e:
                print(f"Error while deleting {filename}: {str(e)}")



    print("message_history after question :", message_history)
    input_token_usage_cost = round(input_tokens_conv * 0.000003, 2)
    output_token_usage_cost = round(output_tokens_conv * 0.000015, 2)
    total_token_usage_cost = round(input_token_usage_cost + output_token_usage_cost, 2)
    total_tokens_metric.add(total_tokens_conv)
    total_tokens_cost.add(total_token_usage_cost)
    latency_metric.record(round(latency, 2))
    tokens_per_conv.add(round(total_tokens_conv / 6, 2))
    print("total input tokens",input_tokens_conv)
    print("total output tokens",output_tokens_conv)
    print("total overall tokens",total_tokens_conv)


clickable_handler = ClickableQuestionHandler(handle_user_query)
@cl.action_callback("Clickable_Question")
async def on_clickable_question(action: cl.Action):
    global dataware_house,databricks_auth_token
    if dataware_house is None:
        print("SQL Wareshouse Initialization")
        dataware_house=DataWarehouse(host_name,http_path,databricks_auth_token)
        print("SQL Wareshouse Initialized")
    await clickable_handler.process_clickable_question(action,dataware_house)


