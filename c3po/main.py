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
from chainlit import ChatProfile
import pandas as pd
import base64
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
from CopyFolder import copy_folders

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

import psutil
import time
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def log_memory_usage(interval=60):
    process = psutil.Process()
    while True:
        # Process memory usage
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / 1024 / 1024

        # System memory usage
        virtual_mem = psutil.virtual_memory()
        available_mb = virtual_mem.available / 1024 / 1024
        total_mb = virtual_mem.total / 1024 / 1024
        used_percent = virtual_mem.percent

        logging.info(f"Process Memory Usage (RSS): {rss_mb:.2f} MB")
        logging.info(f"System Available Memory: {available_mb:.2f} MB / {total_mb:.2f} MB ({100 - used_percent:.2f}% free)")

        time.sleep(interval)

# Example usage
# log_memory_usage(interval=60)


# Store the original print function
original_print = builtins.print

# Global variable to store current thread ID
current_thread_id = "NO_THREAD_ID"

"""
Thread ID Logging System
========================
This system automatically includes the current Chainlit thread ID in all print statements.
The thread ID is automatically set when:
1. A new message is received (run_conversation function)
2. A chat starts (start function)
3. A clickable question is processed (on_clickable_question function)

All print statements will now show:
[Thread ID: <thread_id>] [Memory: <memory_usage>] and AVAILABLE MEMORY <available_mb> MB / <total_mb> MB (<free_percent>% free) at time : [<timestamp>]

Example output:
"Databricks Connection established [Thread ID: abc123] [Memory: 45.67 MB] and AVAILABLE MEMORY 2048.00 MB / 8192.00 MB (75.00% free) at time : [2024-01-15 10:30:45]"
"""

def set_current_thread_id(thread_id):
    """Set the current thread ID globally"""
    global current_thread_id
    current_thread_id = thread_id
    print(f"Thread ID set to: {thread_id}")

def get_current_thread_id():
    """Get the current thread ID"""
    global current_thread_id
    return current_thread_id

def update_thread_id_from_context():
    """Update thread ID from Chainlit context if available"""
    try:
        if hasattr(cl, 'context') and cl.context and hasattr(cl.context, 'session'):
            thread_id = cl.context.session.thread_id
            if thread_id:
                set_current_thread_id(thread_id)
                return True
    except Exception as e:
        print(f"Could not get thread ID from context: {e}")
    return False

def print_with_thread_id_and_memory(*args, **kwargs):
    """Override print to show thread ID, memory usage, and the actual print content"""
    # Get current memory usage
    process = psutil.Process()
    mem_info = process.memory_info()
    rss_mb = mem_info.rss / 1024 / 1024
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    virtual_mem = psutil.virtual_memory()
    available_mb = virtual_mem.available / 1024 / 1024
    total_mb = virtual_mem.total / 1024 / 1024
    used_percent = virtual_mem.percent

    # Get thread ID
    thread_id = get_current_thread_id()
    # Print memory first
    # Print with thread ID, memory info, and original content
    original_print(*args, f"[Thread ID: {thread_id}] [Memory: {rss_mb:.2f} MB] and AVAILABLE MEMORY {available_mb:.2f} MB / {total_mb:.2f} MB ({100 - used_percent:.2f}% free) at time : [{timestamp}]", **kwargs)

# Override the built-in print function
builtins.print = print_with_thread_id_and_memory

# Test the new print function
print("Print function override successful - Thread ID should appear in future prints")

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
# loader.load_files_from_s3()
# copy_folders("ONC","Structured_Bot")
BOTS = ast.literal_eval(os.getenv("BOTS"))
print("BOTS:", BOTS, type(BOTS))
from gilead_loader import GileadDataLoader
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

#HCP loader imports
from Structured_Bot.relevant_source_instructions import get_relevant_source_instructions_processor
from Semi_Structured_Bot.opensearch_execution import OpensearchExecutionManager
from Semi_Structured_Bot.HCP_Loader import HCPFilesLoader
hcp_instructions = HCPFilesLoader.load_instructions()
opensearch_hybrid_query_generation_prompt = hcp_instructions.get("opensearch_hybrid_query_generation_prompt","")
tools_Semi_Structured = HCPFilesLoader.load_tools().get("functions_hcp","")
opensearch = OpensearchExecutionManager()

#BYOD imports
from BYOD.byod_loader import BYODFilesLoader
from BYOD.document_reader import FileReader
file_reader = FileReader()
from BYOD.document_processor import BYODProcessor
byod_processor = BYODProcessor()
byod_instructions = BYODFilesLoader.load_instructions()
byod_system_prompt = byod_instructions.get("byod_system_prompt","")
tools_byod = []

#tools exectuion imports
from ToolCalls import ToolCallsExecutor
tool_executor = ToolCallsExecutor()
from Files_Images_Handling import ElementsHandling
files_images_handler=ElementsHandling()
from chainlit.oauth_providers import providers,get_oauth_provider
from date_fetching import get_date_instruction


session = get_session()

BOT_TYPE_STRUCTURED = os.getenv("BOT_TYPE_STRUCTURED")
BOT_TYPE_SEMI_STRUCTURED = os.getenv("BOT_TYPE_SEMI_STRUCTURED")
BOT_TYPE_BYOD = os.getenv("BOT_TYPE_BYOD")
index_name_hcp = os.getenv("INDEX_NAME_HCP")
index_name_byod = os.getenv("INDEX_NAME_BYOD")
BOT_TYPE_PMR=os.getenv("BOT_TYPE_PMR")
BOT_TYPE_EARLY_EXP=os.getenv("BOT_TYPE_EARLY_EXP")
BOT_TYPE_PBC_MARKET_RESEARCH=os.getenv("BOT_TYPE_PBC_MARKET_RESEARCH")
BOT_TYPE_RM_INSIGHTS=os.getenv("BOT_TYPE_RM_INSIGHTS")
BOT_TYPE_MARKET_MAP=os.getenv("BOT_TYPE_MARKET_MAP")
BOT_TYPE_PHYSICIAN_OPINIONS_V2=os.getenv("BOT_TYPE_PHYSICIAN_OPINIONS_V2")
BOT_TYPE_DSG = os.getenv("BOT_TYPE_DSG")
BOTS = ast.literal_eval(os.getenv("BOTS"))
print("BOTS:", BOTS, type(BOTS))


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
model_id=os.getenv("model_id")
print("Model ID: in main from env : ", model_id)
# import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
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


# execution_manager = PythonExecutionManager(gilead_loader)
def load_files(folders):
    data = GileadDataLoader.load_multiple_folders(folders)
    return data


# repl_tool = PythonExecutionManager(gilead_loader)
print("after loading all instructions")
from Structured_Bot.handle_clickable_questions import ClickableQuestionHandler
from Structured_Bot.sending_req_to_llm import RequestLLM
from Structured_Bot.chatbot_handler import ChatbotHandler 
from Structured_Bot.llm_requests_new import LLM
llm_request = LLM()
# execution_manager = PythonExecutionManager(gilead_loader)
chatbot_handler = ChatbotHandler()

sending_request_to_llm = RequestLLM()

import opentelemetry.instrumentation.openai.shared.chat_wrappers as chat_wrappers

def safe_accumulate_stream_items(item, complete_response):
    # This is a copy of the original function, but with a safe check
    # You may need to copy the full function from the library and patch only the relevant line
    # For demonstration, here's a minimal patch:
    for i, complete_choice in enumerate(complete_response["choices"]):
        if (
            "tool_calls" in complete_choice["message"]
            and isinstance(complete_choice["message"]["tool_calls"], list)
            and len(complete_choice["message"]["tool_calls"]) > i
        ):
            span_tool_call = complete_choice["message"]["tool_calls"][i]
            # ... rest of the original logic ...
        else:
            # skip or handle gracefully
            continue

# Patch the function
chat_wrappers._accumulate_stream_items = safe_accumulate_stream_items


from sql_warehouse_handling import DataWarehouse
http_path =get_secret(secret_name,key="DATABRICKS_HTTP_PATH")
host_name=get_secret(secret_name,key="DATABRICKS_SERVER_HOSTNAME")
# sql_warehouse()


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
    if provider_id == "databricks":
        # Store in USER SESSION, not global variable
        #cl.user_session.set("databricks_token", token)
        os.environ["INITIAL_ACCESS_TOKEN"]=token
        
        user = cl.User(
            identifier=default_user.identifier,
            metadata={
                **default_user.metadata,
                "access_token": token,
                "provider": provider_id,
            }
        )

    result = loader.load_files_from_s3()
    print("after refresh of data in auth")
    if result:
        print("Files Modified at S3")
    else:
        print("Files Was not Modified at S3")

    return default_user

print("Before authorization")
@cl.password_auth_callback
def auth_callback(username: str, password: str):# -> Optional[cl.AppUser]:
  # Fetch the user matching username and compare the password with the value stored in the database
  usernames_passwords_lst = [("shashi","shashi"),('husna.siddiqa@setuserv.com', 'setuserv123'), ('kalyankumar.marella@setuserv.com', 'setuserv123'), ('sanga', 'setuserv123'), ('sanjeev.kayath@gilead.com', 'gilead123'), ('gaurav.bhatnagar@gilead.com', 'gilead123'), ('sadhna.thakur5@gilead.com', 'gilead123'), ('Badari.Ganti@gilead.com', 'gilead123'), ('karthik.jodu@setuserv.com', 'setuserv123'), ('shashikumar.vemula@setuserv.com','setuserv123'),('samvidha.reddy@setuserv.com', 'setuserv123'), ('parashuram.reddy@setuserv.com','setuserv123'), ('Sumit.Singh70@gilead.com', 'gilead123'), ('sanga@setuserv.com', 'setuserv123'), ('srikanth.reddy@setuserv.com', 'setuserv123'), ('kristi.pedersen@gilead.com', 'gilead123'), ('SangaReddy.Peerreddy@gilead.com', 'gilead123'), ('diego.dimes@gilead.com', 'gilead123'), ('samvidha.reddy@setuserv.com', 'setuserv123'),('sayantan.dasgupta@arvinas.com','arvinas123'),('yshankar@prescriptiveinsights.com','prescriptive123'),('suresh.divakar@prescriptiveinsights.com','prescriptive123'),('avani.patlolla@setuserv.com','setuserv123'),('chandana.rajarapu@setuserv.com','setuserv123'),('vikram@multiplierai.com','multiplierai123'),('guest@setuserv.com','setuserv123'),('nalini.purkayastha@gilead.com','gilead123'),('shomita.mandal@gilead.com','gilead123'),('pete.bielecki@gilead.com','gilead123'),('carolynn.chang@gilead.com','gilead123'),('nadia.cole@gilead.com','gilead123')]
  new_lst = []
  for tup in usernames_passwords_lst:
      new_lst.append((tup[0].lower(), tup[1].lower()))
  if (username.lower(), password.lower()) in new_lst:
    #cl.user_session.set("username", username)
    result = False
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


async def run_parallel_llm_calls(gilead_prompt, user_message_copy, fields, queries_code, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency, chatbot_name):
    """Run both LLM calls in parallel and combine results"""
    print("all business rules : ", gilead_prompt)
    data = load_files(BOTS)
    # Execute both calls simultaneously 
    results = await asyncio.gather(
        sending_request_to_llm.get_relevant_business_rules(
            data[chatbot_name]['instructions']['get_relevant_business_rules'], gilead_prompt, user_message_copy, fields, email, 
            total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
        ),
        sending_request_to_llm.pick_relevant_example_queries(
            data[chatbot_name]["instructions"]["relevant_query_picking_agent"],user_message_copy, queries_code, email, 
            total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
        )
    )
    
    # Unpack results
    (relevent_business_rules, email, total_tokens_1, input_tokens_1, output_tokens_1, latency_1) = results[0]
    (relevant_examples, email, total_tokens_2, input_tokens_2, output_tokens_2, latency_2) = results[1]
    
    # Combine metrics
    combined_total_tokens = total_tokens_1 + total_tokens_2
    combined_input_tokens = input_tokens_1 + input_tokens_2
    combined_output_tokens = output_tokens_1 + output_tokens_2
    combined_latency = max(latency_1, latency_2)
    
    return (relevent_business_rules, relevant_examples, email, 
            combined_total_tokens, combined_input_tokens, combined_output_tokens, combined_latency)




@cl.step

@cl.on_chat_start
async def start():
    
    cl.user_session.set("byod_doc_flag", "false")
    cl.user_session.set("ingested_flag", None)

    # Set the current thread ID for print statements
    update_thread_id_from_context()

    
    tool_params = {"arguments":{}, "dataware_house":{}, "user_message":{}, "result":{}, "namespace":{}}
    cl.user_session.set('tool_params',tool_params)
    cl.user_session.set("databricks_auth_token",os.getenv("DATABRICKS_TOKEN"))
    # cl.user_session.set("databricks_auth_token",None)
    user_data=cl.user_session.get("user")
    if user_data.metadata['provider']=='databricks':
        cl.user_session.set('dataware_house',None)
        cl.user_session.set("databricks_auth_token",user_data.metadata['access_token'])
    databricks_auth_token=cl.user_session.get("databricks_auth_token")
    if databricks_auth_token is not None:
        pass
    else:
        redirect_url=providers[-1].redirect_uri
        domain=redirect_url[:redirect_url.find("/auth")]
        url=f"{domain}/auth/oauth/databricks"
        html_content = f"""
        <span style="font-size: 16px; color: #ccc;">
            Please Authenticate
            <form action="{url}" method="GET" style="display:inline;">
                <button type="submit" class="dbx-login-btn" style="
                    background-color: #e53935;
                    color: white;
                    padding: 2px 10px;
                    border: none;
                    border-radius: 3px;
                    font-size: 13px;
                    font-weight: 600;
                    cursor: pointer;
                    margin: 0 2px;
                    display: inline;
                    vertical-align: middle;
                    transition: background 0.2s, transform 0.1s, box-shadow 0.2s;
                ">Continue with Databricks</button>
            </form>
            <style>
                .dbx-login-btn:hover {{
                    background-color: #b71c1c;
                    box-shadow: 0 2px 8px rgba(229,57,53,0.25);
                    transform: translateY(-2px) scale(1.04);
                }}
                .dbx-login-btn:active {{
                    background-color: #d32f2f;
                    transform: translateY(1px) scale(0.98);
                    box-shadow: 0 1px 2px rgba(229,57,53,0.15);
                }}
            </style>
        </span>
        """
        await cl.Message(content=html_content).send()

    await chatbot_handler.start_chat()

session_boto3=boto3.Session()
s3_client = boto3.client('s3',region_name="us-west-2",config=Config(signature_version='s3v4'))
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
                'role': 'user',
                'content': content
            })
        elif step_type == 'llm' and content != '' and content !='Data Retrieved Successfully':
            message_history.append({
                'role': 'assistant',
                'content': content,
                'tool_calls': step.get('tool_calls', [])
            })
        elif step_type == 'run' and content != '' and content !='Data Retrieved Successfully':
            message_history.append({
                'role': 'tool',
                'content': content,
                'tool_call_id': step.get('id', '')
            })
    
    return message_history


@cl.on_settings_update
async def update_settings(settings):
    await cl.Message(content="Bot Switching ... Please wait").send()
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


async def format_partial(template, **kwargs):
    class PartialFormatter(dict):
        def __missing__(self, key):
            return '{' + key + '}'
        
        def __getitem__(self, key):
            try:
                value = super().__getitem__(key)
                # If the value is a string and the key contains a dot (like EQ_APR_NR.shown),
                # return the original placeholder to avoid attribute access errors
                if isinstance(value, str) and '.' in key:
                    return '{' + key + '}'
                return value
            except KeyError:
                return '{' + key + '}'
    
    # Handle nested attribute access by replacing {key.attr} with {key} when base key
    # is missing or maps to a string. This avoids Python trying to access attributes on strings.
    import re
    def replace_nested_placeholders(text):
        pattern = r'\{([^}]+)\}'
        def replace_func(match):
            placeholder = match.group(1)
            if '.' in placeholder:
                base_key, _, attr = placeholder.partition('.')
                base_val = kwargs.get(base_key, None)
                if base_key not in kwargs:
                    return '{' + base_key + '}'
                if isinstance(base_val, str) or not hasattr(base_val, attr):
                    return '{' + base_key + '}'
            return match.group(0)
        return re.sub(pattern, replace_func, text)

    template = replace_nested_placeholders(template)

    # Escape non-placeholder braces to prevent format spec errors (e.g., JSON-like {"key": "value"})
    identifier_pattern = re.compile(r'^[a-zA-Z_][\w.]*$')
    def escape_non_placeholders(text):
        def esc_func(match):
            inner = match.group(1)
            if inner in kwargs or identifier_pattern.match(inner):
                return match.group(0)
            # Escape if contains characters typical of JSON/dict or composite tokens
            if (':' in inner or '"' in inner or "'" in inner or ' ' in inner or ',' in inner):
                return '{{' + inner + '}}'
            return match.group(0)
        return re.sub(r'\{([^}]+)\}', esc_func, text)

    template = escape_non_placeholders(template)

    return template.format_map(PartialFormatter(kwargs))


async def format_partial_new(template, **kwargs):
    """
    Format template with partial placeholder replacement.
    Protects {{double braces}} and complex JSON structures.
    """
    import re
    
    # Step 1: Protect double braces by temporarily replacing them
    protected_template = template.replace('{{', '\x00DOUBLE_OPEN\x00')
    protected_template = protected_template.replace('}}', '\x00DOUBLE_CLOSE\x00')
    
    # Step 2: Replace only the provided single-brace placeholders
    for key, value in kwargs.items():
        # Match {key} with word boundaries
        pattern = r'\{' + re.escape(key) + r'\}'
        protected_template = re.sub(pattern, str(value), protected_template)
    
    # Step 3: Restore double braces as single braces
    result = protected_template.replace('\x00DOUBLE_OPEN\x00', '{')
    result = result.replace('\x00DOUBLE_CLOSE\x00', '}')
    
    return result

async def process_prompt_dsg(chatbot_name, data):
    system_prompt = "YOU ARE AN ANALYST FOR A PHARMA COMPANY. SO USING THE BELOW INFORMATION OF TABLES AND RULES YOU GENERATE THE SQL QUERY, VISUALIZE THE DATA AND GENERATES THE INSIGHTS TO THE USER\n"
    system_prompt += "TABLE 1: stg_iqvia_laad_rx_onc_hist_preprocessed \n Column Descriptions : " + str(data[chatbot_name]["tools"].get("rx_data_column_descriptions","")) + "\n"
    system_prompt += "TABLE 2: stg_iqvia_laad_mx_onc_hist_preprocessed \n Column Descriptions : " + str(data[chatbot_name]["tools"].get("mx_data_column_descriptions","")) + "\n"
    system_prompt += "TABLE 3: breast_cancer_drugs_ndc_codes_mapped \n Column Descriptions : " + str(data[chatbot_name]["tools"].get("ndc_mapping_file_column_descriptions","")) + "\n"
    system_prompt += "TABLE 4: breast_cancer_drugs_hcpcs_codes_mapped \n Column Descriptions : " + str(data[chatbot_name]["tools"].get("hcpcs_mapping_file_column_descriptions","")) + "\n"
    system_prompt += "TABLE 5: nda_sales_nsp_monthly_stg_hist \n Column Descriptions : " + str(data[chatbot_name]["tools"].get("nsp_data_column_descriptions","")) + "\n"
    system_prompt += data[chatbot_name]["instructions"].get("business_rules_instructions","")
    return system_prompt


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
    global thread_id
    thread_id = message.thread_id
    # Set the current thread ID for print statements
    set_current_thread_id(thread_id)
    
    total_tokens_conv=0
    input_tokens_conv=0
    output_tokens_conv=0
    global latency
    latency=0
    start_time = time.time()
    email = cl.user_session.get("user").identifier

    #Extract the user's query from the message
    query = message.content
    cl.user_session.set("user_message",query.strip())
    print("user question: ", query)
    databricks_auth_token=cl.user_session.get("databricks_auth_token")
    # databricks_token=cl.user_session.get("databricks_token")
    print("databricks token in run_conversation",databricks_auth_token)
    cl.user_session.set("coding_language","SQL")
    # Retrieve the message history from the user session

    message_history = cl.user_session.get("message_history")
    print("message_history!!!!!!!!!!!!!!",len(message_history),  message_history)

    # Retrieve the active chatbot name from the session
    chatbot_name = cl.user_session.get("chat_profile")
    print("chatbot_name!!!!!!!!!!!", chatbot_name)
    
    data = load_files(BOTS)
    print("data in run_conversation", data.keys())

    # Check if the active chatbot is "Structured"
    if chatbot_name == BOT_TYPE_STRUCTURED or chatbot_name == BOT_TYPE_RM_INSIGHTS or chatbot_name == BOT_TYPE_DSG:

        print("data in chatbot_name", data.keys())

        print("in chatbot_name !!!")

        if databricks_auth_token is None:
            redirect_url=providers[-1].redirect_uri
            domain=redirect_url[:redirect_url.find("/auth")]
            url=f"{domain}/auth/oauth/databricks"
            html_content = f"""
            <span style="font-size: 16px; color: #ccc;">
                Please Authenticate
                <form action="{url}" method="GET" style="display:inline;">
                    <button type="submit" class="dbx-login-btn" style="
                        background-color: #e53935;
                        color: white;
                        padding: 2px 10px;
                        border: none;
                        border-radius: 3px;
                        font-size: 13px;
                        font-weight: 600;
                        cursor: pointer;
                        margin: 0 2px;
                        display: inline;
                        vertical-align: middle;
                        transition: background 0.2s, transform 0.1s, box-shadow 0.2s;
                    ">Continue with Databricks</button>
                </form>
                <style>
                    .dbx-login-btn:hover {{
                        background-color: #b71c1c;
                        box-shadow: 0 2px 8px rgba(229,57,53,0.25);
                        transform: translateY(-2px) scale(1.04);
                    }}
                    .dbx-login-btn:active {{
                        background-color: #d32f2f;
                        transform: translateY(1px) scale(0.98);
                        box-shadow: 0 1px 2px rgba(229,57,53,0.15);
                    }}
                </style>
            </span>
            """
            await cl.Message(content=html_content).send()
        
        else:
            active_bot = chatbot_name
            dataware_house=cl.user_session.get('dataware_house')
            databricks_auth_token=cl.user_session.get("databricks_auth_token")
            if dataware_house is None and databricks_auth_token:
                print("SQL Wareshouse Initialization")
                dataware_house=DataWarehouse(host_name,http_path,databricks_auth_token)
                print("SQL Wareshouse Initialized")
                cl.user_session.set("dataware_house",dataware_house)
            tool_params=cl.user_session.get('tool_params')
            tool_params["dataware_house"]=cl.user_session.get("dataware_house")
            cl.user_session.set("tool_params",tool_params)

            thread_data = await data_layer_instance.get_thread(thread_id)
            message_history=extract_message_history(thread_data)
            cl.user_session.set("message_history",message_history)
            
            # If there is a message history, revise the query
            if len(message_history) > 1:
                # revised_query = await sending_request_to_llm.get_revised_query(revised_query_prompt, message_history, query)
                revised_query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.get_revised_query(data[chatbot_name]["instructions"].get("revised_query_instructions"), message_history, query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency)
            else:
                revised_query = query # No revision needed for the first query
            print("The query is!!!!!!!!!!!", query)
            print("revised_query!!!!!!!!!!!!!", revised_query)

            await stream_on_ui("Revised query : "+revised_query)

            print("This is the revised query",revised_query)
            active_bot = chatbot_name
            if active_bot == BOT_TYPE_STRUCTURED or active_bot == BOT_TYPE_DSG:
                intent_clarify_prompt=data[active_bot]["instructions"].get("Intent_clarify_instructions")
                intent_clarify_prompt+="\n"+ get_date_instruction(cl.user_session.get("dataware_house"))


                print("Here are keys of instructions for the intent module : ",data[active_bot]["instructions"].keys())
                if active_bot==BOT_TYPE_STRUCTURED:
                    intent_clarify_prompt=intent_clarify_prompt.format(sales_instructions=data[active_bot]["instructions"].get("DataSource_Sales_Instructions"),claims_instructions=data[active_bot]["instructions"].get("DataSource_Claims_Instructions"),data_source_instructions=data[active_bot]["instructions"].get("get_data_source"),question=revised_query)
                if active_bot == BOT_TYPE_DSG:
                    intent_clarify_prompt = intent_clarify_prompt.format(question=revised_query)


                intent_question_history=None
                intent_question_history=[{"role": "system", "content":intent_clarify_prompt},{"role": "user", "content": revised_query}]
                cur=0
                while cur<5:
                    Intent_claude_message = {"role": "", "content": ""}
                    Intent_function_ui_message = None
                    Intent_content_ui_message = cl.Message(content="")
                    intent_tools = data[active_bot]["tools"]["intent_tool_functions"]
                    print("here are the activate_bot_functions ...",intent_tools)
                    try:
                        response = await request_llm.send_request_streaming(intent_question_history,tools=data[active_bot]["tools"]["intent_tool_functions"])
                        print("intent question history !!!!!", intent_question_history)
                        print("tool calls after call",intent_tools)
                        
                    except Exception as e:
                        print("error in calling intent agent",e)
                    async for stream_resp in response:
                        # print("stream response choices 0 intent",stream_resp.choices[0])
                        # print("stream_response intent= ",stream_resp.choices[0].finish_reason)
                        new_delta = stream_resp.choices[0].delta
                        Intent_claude_message, Intent_content_ui_message, Intent_function_ui_message,stream_resp= await UI_Utils.process_new_delta(
                            new_delta, Intent_claude_message, Intent_content_ui_message, Intent_function_ui_message, stream_resp,True)
                    # Ensure assistant message content is non-empty when appending
                    if not Intent_claude_message.get("content"):
                        Intent_claude_message["content"] = " "
                    intent_question_history.append(Intent_claude_message)
                    if stream_resp.choices[0].finish_reason == "tool_calls":
                        # print("iam in tool calls for intent questions")
                        function_name = Intent_claude_message.get("tool_calls", [{}])[0].get("function", {}).get("name", "")
                        print("here is the function name selected ",function_name)
                        tool_call_id = Intent_claude_message.get("tool_calls", {})[0].get("id", "")
                        argument_str = Intent_claude_message.get("tool_calls", [{}])[0].get("function", {}).get("arguments", "")
                        print("here is the argument str of tool calls husna",argument_str)

                    if stream_resp.choices[0].finish_reason=="stop":
                        print("please ask questions related to data analysis")
                        # await cl.Message("please ask questions related to data analysis").send()
                        break
                    print("claude message for intent query",Intent_claude_message)

                    if function_name=="clarify_user_intent":
                        argument_str = Intent_claude_message.get("tool_calls", [{}])[0].get("function", {}).get("arguments", "")
                        try:
                            args = json.loads(argument_str) if argument_str else {}
                        except json.JSONDecodeError as e:
                            print("Error parsing arguments:", e)
                            return None

                        # Now you can access the revised_query from the parsed dictionary
                        unclear_aspects = args.get("unclear_aspects")
                        casual_flag=args.get("casual_flag")
                        casual_message=args.get("casual_message")
                        clarifying_questions = args.get("clarifying_questions")

                        print("here is the casual flag",casual_flag)
                        print("here is the casual message",casual_message)
                        if casual_flag=="YES":
                            function_response=casual_message
                            await stream_on_ui(casual_message)
                            return
                        

                        # clarifying_questions = clarifying_questions.split("?")
                        # clarifying_questions = [q.strip() + '?' for q in clarifying_questions if q]
                        # clarifying_questions_str = "\n- ".join(clarifying_questions)

                        # print("here are the clarifying question",clarifying_questions)
                        # print("here is the clarifying question str",clarifying_questions_str)
                        # await stream_on_ui(clarifying_questions_str)


                        # message_content = f"""Could you please clarify the above so I can assist you better?"""

                        res = await cl.AskUserMessage(
                        content=f"Please clarify the question for unclear aspects: {unclear_aspects}", timeout=180).send()

                        if res:
                            # Process the user's response
                            print(f"Received response: {res['output']}")
                            user_response = res['output']
                            print(f"User responded: {user_response}")
                            function_response=user_response
                    elif function_name=="Intent_revise_query":
                        print("revised the question",Intent_claude_message)
                        args=Intent_claude_message.get("tool_calls", [{}])[0].get("function", {}).get("arguments", "")
                            # If the arguments field is a JSON string, parse it into a dictionary
                        try:
                            args = json.loads(args) if args else {}
                        except json.JSONDecodeError as e:
                            print("Error parsing arguments:", e)
                            return None

                        # Now you can access the revised_query from the parsed dictionary
                        revised_intent_query = args.get("revised_query")
                        print("clarified question :------", revised_intent_query)
                        function_response=revised_intent_query
                        revised_query=revised_intent_query
                        print("here is the revised query inside",revised_query)
                        strg="Clarified Question: "+revised_intent_query
                        await cl.Message(content=strg).send()
                        break
                    else:
                        print("function name",function_name)
                        function_response="No function found"
                        break
                    intent_question_history.append(
                        {
                            "role": "tool",
                            "content": function_response,
                            'tool_call_id': tool_call_id,
                        }
                    )
                    cur+=1

            if chatbot_name == os.getenv("BOT_TYPE_DSG"):
                system_prompt = await process_prompt_dsg(chatbot_name, data)
                print("SYSTEM PROMPT FOR DS&G BOT : ", system_prompt)
                queries_code = data[chatbot_name]["tools"].get("Queries_Codes","")
                await handle_user_query(revised_query, gilead_prompt=system_prompt, data_source='', fields=[], queries_code=queries_code, email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)

            else:
                print("here is the revised query outside",revised_query)
                config = data[chatbot_name]["configs"]
                print("data : ... ",data["Structured"].keys())
                descriptions = data[chatbot_name]["tools"]
                print("config keys: ",config.keys())
                print("descriptions keys: ",descriptions.keys())
                if len(config["configs"]["list_of_data_sources"])==1:
                    data_source = config["configs"]["list_of_data_sources"][0]
                    column_descriptions = data_source + "_Column_Descriptions"
                    if chatbot_name == BOT_TYPE_RM_INSIGHTS:
                        # RM Insights uses unstructured text; provide known columns directly
                        rm_fields = [
                            "ID",
                            "Question",
                            "Insight",
                            "Sentiment",
                            "Standerdized topic",
                            "Topic of text"
                        ]
                        data_source_dict = {
                            "source": data_source,
                            "fields": rm_fields
                        }
                    else:
                        data_source_dict={
                            "source": data_source,
                            "fields": list(descriptions[column_descriptions].keys())
                        }
                else:
                    # data_source_dict = await sending_request_to_llm.get_data_source_and_fields(revised_query)
                    data_source_dict,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.get_data_source_and_fields(revised_query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency, system_content=data[chatbot_name]['instructions']["get_data_source"])
                
                #Bot started RM_insights
                # Determine the data source and fields
                # if chatbot_name == BOT_TYPE_RM_INSIGHTS:
                #     # RM Insights builds its own prompt later; skip generic processor
                #     data_source = data_source_dict["source"]
                #     fields = data_source_dict["fields"]
                #     gilead_prompt = ""
                #     queries_code = ""
                # else:
                data_source, fields, gilead_prompt, queries_code = await get_relevant_source_instructions_processor(data_source_dict, chatbot_name)
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

    elif chatbot_name == BOT_TYPE_SEMI_STRUCTURED:
        print("in chatbot_name Semi_Structured !!!")
        await handle_user_query(query, gilead_prompt=opensearch_hybrid_query_generation_prompt, data_source='', fields=[], queries_code='', email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)
    
    elif chatbot_name == BOT_TYPE_BYOD:
        byod_system_prompt = byod_instructions.get("byod_system_prompt","")
        thread_id = message.thread_id
        print("thread_id: ", thread_id)
        user_message_copy = query
        chatbot_name=cl.user_session.get("chat_profile")
        print("chatbot_name!!!!!!!!!!!", chatbot_name)
        print("in chatbot_name BYOD !!!")

        byod_doc_flag = cl.user_session.get("byod_doc_flag")
        if byod_doc_flag == "false" and not message.elements:
            await stream_on_ui("No file attached. Please upload a file. to answer the query")
        else:
            if cl.user_session.get("ingested_flag") is None:
                await stream_on_ui("Processing files...") 

            # Process all files
            processed_files = []
            print("message elements: ", message.elements)
            if message.elements:
                for file in message.elements:
                    file_path = file.path
                    file_name = file.name
                    file_extension = os.path.splitext(file_name)[1].lower()
                    print("file_path: ", file_path)
                    print("file_name: ", file_name)
                    print("file_extension: ", file_extension)
                    
                    try:
                        byod_doc_flag = cl.user_session.get("byod_doc_flag")
                        if byod_doc_flag == "false":
                            # Extract text based on file extension
                            print("inside try of BYOD")
                            file_content = await file_reader.read(file_path, file_extension)
                            
                            print(f"File content from {file_name}: {file_content}")

                            # Only add non-empty content
                            if file_content.strip():
                                processed_files.append({
                                    "name": file_name,
                                    "content": file_content
                                })

                                print(f"Extracted content from {file_name}:")

                                response = await byod_processor.process_document_and_ingest(file_content, email, thread_id, index_name_byod)

                                print("Response from BYOD processor:", response)

                                if response:
                                    cl.user_session.set("byod_doc_flag", "true")  # Set the flag to true after processing the first document
                                    # Send the response to the UI
                                    await stream_on_ui(f"Processed the document - {file_name}: {response}")
                                    # time.sleep(5)
                                    relevent_data_from_doc = await opensearch.get_relevant_chunks_from_opensearch(str(email), str(thread_id), user_message_copy)
                                    print("data retrieved from opensearch: ", relevent_data_from_doc)
                                    if relevent_data_from_doc:
                                        print("Got Relevant chunks from OpenSearch:")
                                        byod_system_prompt_final = byod_system_prompt + '\n' + relevent_data_from_doc
                                        print("byod_system_prompt is: ", byod_system_prompt_final)
                                        # print(byod_system_prompt)
                                        await handle_user_query(query, gilead_prompt=byod_system_prompt_final, data_source='', fields=[], queries_code='', email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)

                                else:
                                    await stream_on_ui(f"Failed to process {file_name}.")
                        else:
                            cl.user_session.set("byod_doc_flag", "true")  
                            await stream_on_ui("A document has already been uploaded in this chat. You cannot upload another document here, but you can continue querying the existing one. To upload and work with a new document, please start a new chat.")     

                    except Exception as e:
                        await stream_on_ui(f"Error processing {file_name}: {str(e)}")
            
            else:
                relevent_data_from_doc = await opensearch.get_relevant_chunks_from_opensearch(str(email), str(thread_id), user_message_copy)
                print("data retrieved from opensearch: ", relevent_data_from_doc)
                if relevent_data_from_doc:
                    print("Got Relevant chunks from OpenSearch:")
                    byod_system_prompt_final = byod_system_prompt + '\n' + relevent_data_from_doc
                    print("byod_system_prompt is: ", byod_system_prompt)
                    # print(byod_system_prompt)
                    await handle_user_query(query, gilead_prompt=byod_system_prompt_final, data_source='', fields=[], queries_code='', email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)
    #RM_insights bot started
    elif chatbot_name == BOT_TYPE_RM_INSIGHTS:
              
                print("in chatbot_name Chart_Audit !!!")
                print("data keys",data.keys())
                
                thread_data = await data_layer_instance.get_thread(thread_id)
                message_history=extract_message_history(thread_data)
                cl.user_session.set("message_history",message_history)
                
                if len(message_history) > 1:
                        print("inside revised query")
                        # revised_query = await sending_request_to_llm.get_revised_query(revised_query_prompt, message_history, query)
                        revised_query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.get_revised_query(data[chatbot_name]["instructions"].get("revised_query_instructions"), message_history, query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency)
                else:
                        revised_query = query # No revision needed for the first query
                print("The query is for CA ", query)
                print("revised_query is for CA ", revised_query)

                await stream_on_ui("Revised query : "+revised_query)

                tool_params=cl.user_session.get("tool_params")
                tool_params["email"]=email
                tool_params["user_question"]=revised_query
                tool_params["insight_draft_prompt"]=data[chatbot_name]["instructions"].get("insight_draft","")
                tool_params["topic_picking_prompt"]=data[chatbot_name]["instructions"].get("topic_picking_prompt","")
                tool_params["standard_topic_prompt"]=data[chatbot_name]["instructions"].get("standard_topic_prompt","")
                tool_params["text_column"]="statement"

                cl.user_session.set("tool_params",tool_params)
                
                print("This is the revised query for CA",revised_query)
                
                # function_response=None
                # tool_call_id = None
                print("active  bot here : ",chatbot_name)
                # intent_clarify_prompt=data[chatbot_name]["instructions"].get("Intent_clarify_instructions")
                # intent_clarify_prompt=intent_clarify_prompt.format(user_question=revised_query)

                # intent_question_history=None
                # intent_question_history=[{"role": "system", "content":intent_clarify_prompt},{"role": "user", "content": revised_query}]
                
                # print("intent history : ",intent_question_history)
                # Intent_claude_message = {"role": "", "content": " "}
                # Intent_function_ui_message = None
                # Intent_content_ui_message = cl.Message(content="")
                # intent_tools = data[chatbot_name]["tools"]["intent_tool_functions"]
                # print("here are the activate_bot_functions ...",intent_tools)
                # try:
                #     response = await request_llm.send_request_streaming(intent_question_history,tools=intent_tools)
                #     print("intent question history !!!!!", intent_question_history)
                #     print("tool calls after call",intent_tools)
                    
                # except Exception as e:
                #     print("error in calling intent agent",e)
                
                # function_name = None
                # async for stream_resp in response:
                #     # print("stream response choices 0 intent",stream_resp.choices[0])
                #     # print("stream_response intent= ",stream_resp.choices[0].finish_reason)
                #     new_delta = stream_resp.choices[0].delta
                #     Intent_claude_message, Intent_content_ui_message, Intent_function_ui_message,stream_resp= await UI_Utils.process_new_delta(
                #         new_delta, Intent_claude_message, Intent_content_ui_message, Intent_function_ui_message, stream_resp,False)
                # intent_question_history.append(Intent_claude_message)
                # if stream_resp.choices[0].finish_reason == "tool_calls":
                #     # print("iam in tool calls for intent questions")
                #     function_name = Intent_claude_message.get("tool_calls", [{}])[0].get("function", {}).get("name", "")
                #     print("here is the function name selected ",function_name)
                #     tool_call_id = Intent_claude_message.get("tool_calls", {})[0].get("id", "")
                #     argument_str = Intent_claude_message.get("tool_calls", [{}])[0].get("function", {}).get("arguments", "")
                #     print("here is the argument str of tool calls",argument_str)

                # if stream_resp.choices[0].finish_reason=="stop":
                #     print("please ask questions related to data analysis")
                #     # await cl.Message("please ask questions related to data analysis").send()
                # print("claude message for intent query",Intent_claude_message)

                # if function_name=="casual_intent":
                #     argument_str = Intent_claude_message.get("tool_calls", [{}])[0].get("function", {}).get("arguments", "")
                #     try:
                #         args = json.loads(argument_str) if argument_str else {}
                #     except json.JSONDecodeError as e:
                #         print("Error parsing arguments:", e)
                #         return None


                #     # if res:
                #     #     # Process the user's response
                #     #     print(f"Received response: {res['output']}")
                #     #     user_response = res['output']
                #     #     print(f"User responded: {user_response}")
                #     #     function_response=user_response
                #     # Now you can access the revised_query from the parsed dictionary
                #     Flag = args.get("Flag")
                #     print("intent flag : ",Flag)
                #     if Flag == 'YES':
                #         casual_message = args.get("casual_message")
                #         function_response = casual_message
                #         await stream_on_ui(casual_message)
                #         intent_question_history.append(
                #             {
                #                 "role": "tool",
                #                 "content": function_response ,
                #                 'tool_call_id': tool_call_id ,
                #             }
                #         )
                #     else:
                await handle_user_query(revised_query, gilead_prompt="", data_source='', fields=[], queries_code='', email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)
                # else:
                    # await handle_user_query(revised_query, gilead_prompt="", data_source='', fields=[], queries_code='', email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)
       
    
    elif chatbot_name in [BOT_TYPE_PMR,BOT_TYPE_MARKET_MAP,BOT_TYPE_EARLY_EXP,BOT_TYPE_PBC_MARKET_RESEARCH]:
        # topic_picking_prompt=data[chatbot_name]["instructions"].get("topic_picking_prompt","")
        # standard_topic_prompt=data[chatbot_name]["instructions"].get("standard_analysis_prompt","")
        thread_id_pmr = message.thread_id
        thread_data_pmr = await data_layer_instance.get_thread(thread_id_pmr)
        print("thread_id_pmr:  ",thread_id_pmr)
        print("thread_data_pmr: ", thread_data_pmr)
        if thread_data_pmr:
            message_history=extract_message_history(thread_data_pmr)
        else:
            message_history=[]
        cl.user_session.set("message_history",message_history)
        
        # If there is a message history, revise the query
        # query = query.lower()
        # if len(message_history) > 1:
            # revised_query = await sending_request_to_llm.get_revised_query(revised_query_prompt, message_history, query)
        revised_query_unprocess,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.get_revised_query(data[chatbot_name]["instructions"].get("revised_query_instructions"), message_history, query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency)
        # else:
        #     revised_query = query # No revision needed for the first query
        print("revised query unprocess" , revised_query_unprocess)
        revised_query=revised_query_unprocess['question']
        category=revised_query_unprocess.get('category',"")


        print("The query is!!!!!!!!!!!", query)
        print("revised_query!!!!!!!!!!!!!", revised_query)
        print("category is !!!!!!!!!!!!!", category)

        await stream_on_ui("Revised query : "+"**"+revised_query+"**")
        
        cl.user_session.set('data',data)
        user_message_copy = revised_query
        # relevent_data = await opensearch.get_relevant_chunks_from_opensearch(str(email), str(thread_id_pmr), user_message_copy,category=category)
        # print("data retrieved from opensearch for pmr: ", relevent_data)
        
        # Apply LLM-based filtering for Physician Opinions v2
        # if chatbot_name == BOT_TYPE_PHYSICIAN_OPINIONS_V2 and opensearch.enable_physician_opinions_filtering:
        #     print("Applying LLM-based filtering for Physician Opinions v2...")
        #     relevent_data = await opensearch.filter_physician_opinions_data_with_llm(relevent_data, user_message_copy)
        #     print("Filtered data for Physician Opinions v2: ", relevent_data)
        # elif chatbot_name == BOT_TYPE_PHYSICIAN_OPINIONS_V2 and not opensearch.enable_physician_opinions_filtering:
        #     print("LLM-based filtering for Physician Opinions v2 is disabled, using original results")
        # relevent_data={}
        # dataframe=pd.DataFrame(relevent_data)
        # for index in dataframe.index:
        #     if chatbot_name == BOT_TYPE_PMR:
        #         dataframe.loc[index,"combined_text"]=f""" Interviewer Question: {dataframe.loc[index,'questions']} \n Respondent Answer: {dataframe.loc[index,'answers']} """
        #     else:
        #         dataframe.loc[index,"combined_text"]=dataframe.loc[index,'Posts']
        # cl.user_session.set("result_from_pmr",dataframe)
        prompt_file="pmr_prompt" if chatbot_name==BOT_TYPE_PMR or chatbot_name==BOT_TYPE_PBC_MARKET_RESEARCH else "pmr_market_map_prompt"
        if chatbot_name==BOT_TYPE_EARLY_EXP:
            prompt_file="pmr_prompt"
        prompt=data[chatbot_name]["instructions"].get(prompt_file,"")
        print(f"thread id for {user_message_copy}",thread_id_pmr)
        visualize_instr="NEVER DO HARD CODED FILTERATIONS AT VISUALIZATION PART AND GIVE INSIGHTS IN DESCENDING ORDER OF TOPIC COUNTS(USE ANALYSIS DATA FOR SORTING TOPICS IN DESENDING ORDER)"
        prompt= await format_partial_new(prompt,user_question=user_message_copy+visualize_instr,thread_id=thread_id_pmr)
        print("prompt for PMR/marketmap/pbc",prompt)
        tool_params=cl.user_session.get('tool_params')
        tool_params["text_column"]='combined_text'
        # tool_params["result"]=dataframe
        tool_params['user_question']=user_message_copy
        tool_params['insights_prompt']=data[chatbot_name]["instructions"].get("insights_module_instructions","")
        tool_params['email']=''
        tool_params["insight_draft_prompt"]=data[chatbot_name]["instructions"].get("insight_draft","")
        tool_params["topic_picking_prompt"]=data[chatbot_name]["instructions"].get("topic_picking_prompt","")
        tool_params["insight_draft_prompt"]=data[chatbot_name]["instructions"].get("insight_draft","")
        tool_params["standard_topic_prompt"]=data[chatbot_name]["instructions"].get("standard_topic_prompt","")
        tool_params["topic_grouping_prompt"]=data[chatbot_name]["instructions"].get("topic_grouping_prompt","")
        # Provide sentiment prompt for sentiment extraction in TextProcessor
        tool_params["sentiment_prompt"]=data[chatbot_name]["instructions"].get("sentiment_prompt","")
        cl.user_session.set("tool_params",tool_params)
        tool_params["user_message"]=user_message_copy
        await handle_user_query(user_message_copy+visualize_instr, gilead_prompt=prompt, data_source='', fields=[], queries_code='', email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency)



    else:
        print("chatbot_name is not structured or Semi_Structured and BYOD")    
        await handle_user_query(query,data_source='', fields=[], gilead_prompt='', queries_code=[],email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv,latency=latency )

    end_time=time.time()
    response_time=end_time-start_time
    print("response time",response_time)
    response_time_metric.add(response_time)


def encode_image_to_base64(image_path):
    try:
        with open(image_path, "rb") as img_file:
            encoded = base64.b64encode(img_file.read()).decode('utf-8')
            return f"data:image/png;base64,{encoded}"
    except Exception as e:
        print(f"Error encoding image: {e}")
        return "🤖"  # Fallback to emoji

# Encode your images once
structured_icon = encode_image_to_base64("./public/favicon.png")
semi_structured_icon = encode_image_to_base64("./public/r2d2.png")
byod_icon = encode_image_to_base64("./public/bebe_yoda.jpg")



chat_profiles = [
    ChatProfile(
        name="Structured",
        markdown_description="STRUCTURED BOT: Specialized in ONC structured data analysis, SQL queries, and business intelligence",
        icon=structured_icon,
    ),
    # ChatProfile(
    #     name="Semi_Structured",
    #     markdown_description="SEMI-STRUCTURED BOT: Focused on semi-structured data, HCP opinions, and hybrid queries",
    #     icon=semi_structured_icon,
    # ),
    ChatProfile(
        name="BYOD",
        markdown_description="BYOD BOT: Handles Bring Your Own Data scenarios, processing user-uploaded documents and queries",
        icon=byod_icon,
    ),
    ChatProfile(
        name="PMR",
        markdown_description="PMR BOT: Specialized in Primary Market Research analysis and insights",
        icon=structured_icon,
    ),
    # ChatProfile(
    #     name="RM_insights",
    #     markdown_description="RM BOT: Specialized in Patient Market Research analysis and insights",
    #     icon=semi_structured_icon,
    # ),
    # ChatProfile(
    #     name="Physician_opinions_v2",
    #     markdown_description="Physician_opinions_v2 BOT: Specialized in Patient Market Research analysis and insights",
    #     icon=structured_icon,
    # ),
    ChatProfile(
        name="DS&G",
        markdown_description="DS&G BOT: DATA STRATEGY AND GOVERNANCE",
        icon=structured_icon,
    ),
    # ChatProfile(
    #     name="PMR_MARKET_MAP",
    #     markdown_description="MARKET MAP: Specialized in Market Research analysis and insights",
    #     icon=structured_icon,
    # ),
    ChatProfile(
        name="PMR_EARLY_EXPERIENCE",
        markdown_description="EARLY EXP: Specialized in Early Exploratory Research analysis and insights",
        icon=structured_icon,
    ),
    ChatProfile(
        name="PBC_MARKET_RESEARCH",
        markdown_description="PBC MARKET RESEARCH: Specialized in PBC Market Research analysis and insights",
        icon=structured_icon,
    ),
]

@cl.set_chat_profiles
async def chat_profile():
    return chat_profiles


@cl.on_chat_resume
async def on_chat_resume(thread):
    # print("Here is the thread for the on chat resume",thread)
    pass


async def extract_questions_from_examples(queries_code):
    """Extract only Question ID and Question for the picking LLM call"""
    if not isinstance(queries_code, dict):
        return queries_code
    
    return [
        {
            "Question ID": question_id,
            "Question": question_data.get("Question", "")
        }
        for question_id, question_data in queries_code.items()
        if isinstance(question_data, dict)
    ]

async def get_examples_from_ids(relevant_question_ids_list, queries_code):
    """Fetch examples based on the provided IDs from the queries_code dictionary."""
    examples = []
    for q_id in relevant_question_ids_list:
        if q_id in queries_code:
            question_data = queries_code[q_id]
            example = f"Question: {question_data.get('Question', '')}\n CORRECT SQL Query: {question_data.get('Correct Code', '')}"
            examples.append(example)
    return "\n\n".join(examples)

async def is_deck_or_outlier_question(question, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency, chatbot_name, data):
    print("Checking if the question is an outlier...")
    question_type, email, total_tokens_conv, input_tokens_conv, output_tokens_conv = await request_llm.send_request_non_streaming(data[chatbot_name]["instructions"].get("deck_or_outlier_question_detection_prompt"), question, email, total_tokens_conv, input_tokens_conv, output_tokens_conv)
    print("Outlier question detection result: ", question_type)
    if question_type.strip().upper() == "DECK":
        print("✅ Question classified as: DECK CREATION")
        # Call create_presentation_deck tool
        return "deck", email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
    elif question_type.strip().upper() == "OUTLIER":
        print("✅ Question classified as: OUTLIER DETECTION")
        # Call outlier detection flow
        return "outlier", email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
    else:
        print("✅ Question classified as: ANALYTICAL")
        return "analytical", email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
    

@task(name="handling user query")
async def handle_user_query(user_message, data_source='', fields=[],  gilead_prompt='',email='', queries_code='',total_tokens_conv=0,input_tokens_conv=0,output_tokens_conv=0,latency=0):
    email = cl.user_session.get("user").identifier
    chatbot_name = cl.user_session.get("chat_profile")
    user_message_copy = user_message
    tool_params=cl.user_session.get('tool_params')
    tool_params["user_message"] = user_message
    cl.user_session.set("tool_params",tool_params)

    # Set the current thread ID for print statements
    # Get thread ID from the current context if available
    update_thread_id_from_context()

    message_history = cl.user_session.get("message_history")
    data = load_files(BOTS)
    print("data in handle_user_query", data.keys())


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
    
        chatbot_name = cl.user_session.get("chat_profile")
        message_history = cl.user_session.get("message_history")
        print("gilead_prompt structure:", gilead_prompt)
        gilead_prompt+="\n"+ get_date_instruction(cl.user_session.get("dataware_house"))
        # relevent_business_rules = await sending_request_to_llm.get_relevant_business_rules(gilead_prompt, user_message_copy,fields)

        relevent_business_rules, relevant_examples, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency = await run_parallel_llm_calls(gilead_prompt, user_message_copy, fields, queries_code, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency, chatbot_name)  
        print("the relevant business rules picked are: ",relevent_business_rules)
        print("The relevant examples are: ",relevant_examples)

        await stream_on_ui("Fetched the relevent instructions and examples for the query... ")
        await stream_on_ui("Now Proceeding with the next steps...")

        system_prompt = data[chatbot_name]["instructions"].get("get_analysis_plan_nd_code").format(fields = fields, code_instructions = data[chatbot_name]["instructions"].get("sql_code_generation_instructions"), business_rules = relevent_business_rules, relevant_examples=relevant_examples)
        system_prompt +="\n"+"** Insights Response Prompt **"
        insights_header = "Key Insights: " + '\n' * 5
        current_timestamp = datetime.now()
        system_prompt += data[chatbot_name]["instructions"].get("insights_module_instructions")

        print('total system prompt in messages history for the question :',system_prompt)

        print("Insights instructions : ",data[chatbot_name]["instructions"].get("insights_module_instructions"))

        # gilead_prompt += 'Foremost step is to tell which data source you are'
        # message_history[0] = {"role": "system", "content": system_prompt}
        current_question_prompt=[]
        if chatbot_name == BOT_TYPE_STRUCTURED:
            if not current_question_prompt:
                current_question_prompt.append({"role": "system", "content": system_prompt})
            else:
                current_question_prompt[0] = {"role": "system", "content": system_prompt}

    elif chatbot_name == os.getenv("BOT_TYPE_DSG"):

        tool_params['insights_prompt']=data[chatbot_name]["instructions"].get("insights_module_instructions","")
        tool_params['user_question']=user_message_copy

        print("chatbot_name is DSG")

        question_type, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency = await is_deck_or_outlier_question(user_message, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency, chatbot_name, data)
        if question_type == "deck":
            print("question type detected as deck creation")
            system_prompt = data[chatbot_name]["instructions"]["deck_creation_prompt"]

        else:
            await stream_on_ui("Fetching the relevent business rules and examples for the query...")

            relevant_business_rules, relevant_questions_ids, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency = await run_parallel_llm_calls(gilead_prompt, user_message_copy, fields, queries_code, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency, chatbot_name)
            print("RELEVANT BUSINESS RULES PICKED : ",relevant_business_rules)
            print("relevant_questions_ids: ", relevant_questions_ids)

            await stream_on_ui("Fetched the relevent instructions and examples for the query... ")
            await stream_on_ui("Now Proceeding with the next steps...")

            relevant_question_ids_list = eval(relevant_questions_ids[relevant_questions_ids.find("["):relevant_questions_ids.find("]")+1])
            print("relevant_question_ids_list: ", relevant_question_ids_list)

            relevant_examples = await get_examples_from_ids(relevant_question_ids_list, queries_code)
            print("The relevant examples are: ", relevant_examples)

            system_prompt = data[chatbot_name]["instructions"]["avoid_assumptions_prompt"] + relevant_business_rules
            
            system_prompt += "\n\n\n BELOW ARE THE EXAMPLES QUESTIONS AND CORRECT SQL QUERIES FOR REFERENCE: \n\n" + relevant_examples

            print("system prompt DSG in handle user query:", gilead_prompt)

            system_prompt += "\n\n\n BELOW ARE THE SQL CODE AND PYTHON CODE GENERATION INSTRUCTIONS TO GENERATE COMPATIBLE CORRECT SQL QUERIES AND PYTHON CODES : \n\n"
            system_prompt += data[chatbot_name]["instructions"].get("sql_code_generation_instructions","")
            if question_type == "outlier":
                system_prompt += data[chatbot_name]["instructions"].get("outlier_instructions","")
            print("Final system prompt DSG in handle user query:", system_prompt)

        current_question_prompt=[]
        if not current_question_prompt:
            current_question_prompt.append({"role": "system", "content": system_prompt})
        else:
            current_question_prompt[0] = {"role": "system", "content": system_prompt}

    
    elif chatbot_name == BOT_TYPE_SEMI_STRUCTURED:
        print("chatbot_name is HCP_Opinions")
        current_question_prompt=[]
        if not current_question_prompt:
            current_question_prompt.append({"role": "system", "content": opensearch_hybrid_query_generation_prompt})
        else:
            current_question_prompt[0] = {"role": "system", "content": opensearch_hybrid_query_generation_prompt}
        # message_history[0] = {"role": "system", "content": opensearch_hybrid_query_generation_prompt}

    elif chatbot_name == BOT_TYPE_BYOD:
        print("chatbot_name is BYOD")
        current_question_prompt=[]
        if not current_question_prompt:
            current_question_prompt.append({"role": "system", "content": gilead_prompt})
        else:
            current_question_prompt[0] = {"role": "system", "content": gilead_prompt}
        # message_history[0] = {"role": "system", "content": gilead_prompt}
    
    elif chatbot_name == BOT_TYPE_PMR or chatbot_name==BOT_TYPE_MARKET_MAP or chatbot_name==BOT_TYPE_EARLY_EXP or chatbot_name==BOT_TYPE_PBC_MARKET_RESEARCH:
        data = load_files(BOTS)
        current_question_prompt=[]
        if not current_question_prompt:
            current_question_prompt.append({"role": "system", "content": gilead_prompt})
        else:
            current_question_prompt[0] = {"role": "system", "content": gilead_prompt}
        print("PMR system prompt",gilead_prompt)

    elif chatbot_name == BOT_TYPE_PHYSICIAN_OPINIONS_V2:
        data = load_files(BOTS)
        current_question_prompt=[]
        if not current_question_prompt:
            current_question_prompt.append({"role": "system", "content": gilead_prompt})
        else:
            current_question_prompt[0] = {"role": "system", "content": gilead_prompt}
        print("PMR system prompt",gilead_prompt)
    
    

    #RM_insights bot started continues
    elif chatbot_name == BOT_TYPE_RM_INSIGHTS:   
        data = load_files(BOTS)  
        print("chatbot_name is RM_insights",chatbot_name)
        print("data keys", data[chatbot_name].keys())
        print("data tools keys", data[chatbot_name]['tools'].keys())

        message_history = cl.user_session.get("message_history")

        

        # print("intrucstions prompt for survey",data[chatbot_name]['instructions']['relevant_survey_questions_prompt'])
        rm_insights_prompt=await format_partial(data[chatbot_name]['instructions']['relevant_survey_questions_prompt'], user_question=user_message_copy,questions_text=data[chatbot_name]['tools']['unique_survey_questions'])
        # print("rm insights prompt ",rm_insights_prompt)
        print("type:  ", type(cl.user_session))

        await stream_on_ui("Fetching the relevant survey questions...")      
        relevant_questions = await sending_request_to_llm.pick_relevant_example_queries(
            rm_insights_prompt,user_message_copy, queries_code, email, 
            total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
        )
        print("Response : ",relevant_questions)
        question_block = relevant_questions[0]

        def parse_llm_questions_robust(response_text):
            """
            More robust parser that handles various formatting inconsistencies.
            """
            
            lines = response_text.strip().split('\n')
            questions_picked = []
            
            for line in lines:
                line = line.strip()
                
                # Skip empty lines, headers, and separator lines
                if not line:
                    continue
                if 'Questions' in line and 'Question ID' in line:
                    continue
                if re.match(r'^-+$', line):  # Lines with only dashes
                    continue
                    
                # Try to extract question and ID from lines starting with |
                if line.startswith('|'):
                    # Remove leading | and split by |
                    parts = line[1:].split('|')
                    
                    if len(parts) >= 2:
                        question = parts[0].strip()
                        # The ID might be in the second part, extract numbers
                        id_match = re.search(r'\d+', parts[1])
                        
                        if id_match and question and not set(question) <= {'-', ' '}:
                            question_id = id_match.group()
                            questions_picked.append((question, question_id))
            
            return questions_picked
        
        questions_picked = parse_llm_questions_robust(question_block)

        print("here is the result from parsing robust",questions_picked)
        for question, qid in questions_picked:
            print(f"Question: {question}")
            print(f"ID: {qid}")
            print("-" * 50)

        print("parsed questions",questions_picked) 
        question_ui_message = len(questions_picked)
        if question_ui_message == 0:
            msg = "There are 0 survey questions that match to the user question"
        elif question_ui_message == 1:
            msg = "There is 1 survey question that match to the user question"
        else:
            msg = f"There are {question_ui_message} survey questions that match to the user question"

        try:
            await cl.Message(content=msg).send()
            if question_ui_message == 0:
                await cl.Message(content="Can you please rephrase your question or enter a new question ?").send()
        except Exception as e:
            print("here is exception in sending message to ui", e)

    else:
        print("chatbot_name is not structured or Semi_Structured")

    no_of_questions=1
    length=['c3po']
    cnt=0

    if chatbot_name==BOT_TYPE_RM_INSIGHTS:
        no_of_questions=len(questions_picked)


    while cnt < no_of_questions:
        # Ensure per-question iteration state resets each loop
        cur_iter = 0
        saved_files_lst = []
        sql_code=None
        python_code=None
        function_response=None
        cur_iter = 0

        for message in message_history:
            if message['role'] == 'tool':
                message['content'] = " "


        print("after removing function content: ", message_history)

        if len(message_history) > 15:
            del message_history[5:len(message_history) - 9]

        if chatbot_name!=BOT_TYPE_RM_INSIGHTS:
            safe_user_content = user_message_copy if isinstance(user_message_copy, str) and user_message_copy.strip() != "" else " "
            message_history.append({"role": "user", "content": safe_user_content})
            current_history = current_question_prompt[:1] + [{"role": "user", "content": safe_user_content}]


            # current_history = current_question_prompt[:1] + [{"role": "user", "content": user_message_copy}]
            # current_history = message_history[:1] + [{"role": "user", "content": user_message_copy}]
            print('initial current history : 1',current_history,'\n message_history initial : ',message_history)

        if chatbot_name == BOT_TYPE_RM_INSIGHTS:
            current_relevant_question = questions_picked[cnt][0]
            current_relevant_question_id = questions_picked[cnt][1]

            print("here is the current relevent question", current_relevant_question)
            print("here is the current relevent question id", current_relevant_question_id)

            escaped_question = current_relevant_question.replace("'", "\\'")

            # Process only unstructured data
            insights_trigger = data[chatbot_name]['instructions'].get("insights_trigger", "")
            # Enforce correct tool-call order: SQL -> Python -> Insights
            insights_trigger += "\n" + (
                "STRICT ORDER: First call 'generate_sql_code', then 'python_after_sql'. "
                "Only AFTER python completes and CSV/PNG files are saved, call 'insights_response'. "
                "If files are not available, DO NOT call 'insights_response' yet."
            )
            # Safely format the template that contains many braces by escaping all except intended placeholders
            def _safe_format_template(template_str, variables, keep_keys):
                sentinel_map = {k: f"__KEEP_{k.upper()}__" for k in keep_keys}
                for k, token in sentinel_map.items():
                    template_str = template_str.replace("{" + k + "}", token)
                template_str = template_str.replace("{", "{{").replace("}", "}}")
                for k, token in sentinel_map.items():
                    template_str = template_str.replace(token, "{" + k + "}")
                return template_str.format(**variables)

            sql_query_instructions_unstructured = _safe_format_template(
                data[chatbot_name]['instructions']['sql_query_instructions_unstructured'],
                {
                    'user_question': user_message_copy,
                    # Required by template
                    'ID': current_relevant_question_id,
                    'Question': escaped_question,
                    # Optional extras (safe to pass even if unused in template)
                    'survey_question': escaped_question,
                    'input_text': current_relevant_question,
                    'insights': insights_trigger,
                },
                keep_keys=['user_question', 'ID', 'Question', 'insights']
            )
            
            print("query generation prompt : ", sql_query_instructions_unstructured)
            gilead_prompt = sql_query_instructions_unstructured
            
            # Display question to UI
            question_ui_message = f"Question {cnt + 1}: {escaped_question}"
            print("question ui message", question_ui_message)
                

            try:
                await cl.Message(content=f"**{question_ui_message}**").send()
            except Exception as e:
                print("here is exception in sending message to ui", e)

            # Set up message history for unstructured processing
            for message in message_history:
                if message['role'] == 'tool':
                    message['content'] = " "

            print("after removing function content: ", message_history)

            if len(message_history) > 15:
                del message_history[5:len(message_history) - 9]

            # Prepare current question prompt
            current_question_prompt = []
            current_question_prompt.append({"role": "system", "content": gilead_prompt})
            
            print("gilead prompt in rm insights", gilead_prompt)
            
            # Set up current history for processing
            safe_user_content = user_message_copy if isinstance(user_message_copy, str) and user_message_copy.strip() != "" else " "
            message_history.append({"role": "user", "content": safe_user_content})
            current_history = current_question_prompt[:1] + [{"role": "user", "content": safe_user_content}]
            
            print('current history for unstructured processing:', current_history)
            print('message_history:', message_history)

            # Initialize variables for processing
        saved_files_lst = []
        sql_code = None
        python_code = None
        function_response = None
        function_name=None

        while cur_iter < MAX_ITER:

            # OpenAI call
            claude_message = {"role": "", "content": ""}
            function_ui_message = None
            content_ui_message = cl.Message(content="")

            async with cl.Step(name = "C3PO", type = "llm",default_open=False if ("PMR" in chatbot_name or "PBC_MARKET_RESEARCH" in chatbot_name) and function_name!='insights_response' else True) as content_ui_message:#disable_feedback=False,root = True
                async with cl.Step(name="Code " if not sql_code else "Code ", type="llm", show_input= False, language = cl.user_session.get("coding_language"),default_open=False) as function_ui_message:#root=False, 
                    active_bot = chatbot_name
                
                    if active_bot==BOT_TYPE_MARKET_MAP or active_bot == BOT_TYPE_STRUCTURED or active_bot == BOT_TYPE_PMR or active_bot == BOT_TYPE_RM_INSIGHTS or active_bot == BOT_TYPE_PHYSICIAN_OPINIONS_V2 or active_bot==os.getenv("BOT_TYPE_DSG") or active_bot==os.getenv("BOT_TYPE_EARLY_EXP") or active_bot==os.getenv("BOT_TYPE_PBC_MARKET_RESEARCH"):
                        active_bot_functions = data[active_bot]["tools"]["llm_tool_call_functions"]

                    elif active_bot == BOT_TYPE_SEMI_STRUCTURED:
                        active_bot_functions = tools_Semi_Structured

                    else:
                        active_bot_functions = tools_byod

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
                    tool_params=cl.user_session.get("tool_params")
                    tool_params["input_tokens_conv"]=input_tokens_conv
                    tool_params["output_tokens_conv"]=output_tokens_conv
                    tool_params["total_tokens_conv"]=total_tokens_conv
                    if tool_params.get('insights_call',False):
                        cl.user_session.set('insights',claude_message.get('content'))
                    cl.user_session.set('tool_params',tool_params)
                    #print("claude Message for insights: ",claude_message)
                    # print("claude_message: ", claude_message)
                    if not claude_message.get("content"):
                        claude_message["content"] = " "
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
                        tool_params=cl.user_session.get('tool_params')
                        tool_params["arguments"] = arguments
                        cl.user_session.set('tool_params',tool_params)


                if function_name == "calculate_date_ranges"  :
                    python_ui_message = function_name + argument_str
                else:
                    python_ui_message = arguments.get("query")
                    
                print("the python_ui_message is", python_ui_message)
                print("the function_ui_message is", function_ui_message)
                print("the function name called is,",function_name)

                if function_ui_message.output is None:
                    function_ui_message.output = python_ui_message
                await function_ui_message.update()
                                
                tool = tool_executor.get_tool(function_name)
                function_ui_message.output, function_response, result = await tool.run(cl.user_session.get("tool_params"))

                # if result is not None:
                #     tool_params["result"] = result
                if function_name=="insights_response":
                    print("inside insights prompt",function_response)
                    function_response = str(function_response)
                
                if "PMR" in chatbot_name or "PBC_MARKET_RESEARCH" in chatbot_name:
                    function_response = str(function_response)


                if function_ui_message.output is None:
                    function_ui_message.output = python_ui_message
                await function_ui_message.update()
                await cl.sleep(0.1)
                                



                # else:
                #     print("function is not being called")


                safe_tool_content = function_response if isinstance(function_response, str) and function_response.strip() != "" else " "
                print("the safe tool content is", safe_tool_content)
                message_history.append(
                    {
                        "role": "tool",
                        "content": safe_tool_content,
                        'tool_call_id': tool_call_id,
                    }
                )
                current_history.append(
                    {
                        "role": "tool",
                        "content": safe_tool_content,
                        'tool_call_id': tool_call_id,
                    }
                )


                # await cl.Message(
                #     author="Python Code Execution Output",
                #     content = "\n\nAnswer: \n"+str(function_response),
                #     language = "json",
                # ).send()

                if function_name == 'generate_sql_code' or function_name=="python_after_sql" or function_name == "calculate_nsp_capture_ratio": #or function_name_sql=="generate_sql_code" or function_name_sql=="python_after_sql":
                    async with cl.Step(name="Code Execution Output" if sql_code and not python_code else "Code Execution Output", type="llm",show_input= False, language = "Python" if cl.user_session.get("coding_language") == "SQL" else "SQL") as python_code_exc_output:# root=False, 
                        print("in output execution !!!!!!!")
                        python_code_exc_output.output = function_response
                        print("after python_code_exc_output !!!!!!")
                        # await python_code_exc_output.update() 
                        await cl.sleep(0.1)
                    python_code=ast.literal_eval(argument_str).get("query","")
                    print("iam in my files",python_code)
                    images_files_list,excel_files_list,ppt_files_list,pkl_files_list=files_images_handler.get_files_images(python_code,saved_files_lst)
                    tool_params=cl.user_session.get("tool_params")
                    tool_params["excel_files_list"]= excel_files_list
                    tool_params["ppt_files_list"]= ppt_files_list
                    tool_params["images_files_list"]= images_files_list
                    tool_params["pkl_files_list"]= pkl_files_list
                    print("excel_files_list",excel_files_list)
                    print("image files list",images_files_list)
                    print("pkl_files_list",pkl_files_list)
                    cl.user_session.set("tool_params",tool_params)


                if cl.user_session.get('chat_profile') in [BOT_TYPE_PMR,BOT_TYPE_MARKET_MAP,BOT_TYPE_EARLY_EXP,BOT_TYPE_PBC_MARKET_RESEARCH]:

                    # if function_name=="insights_response" and len(images_files_list)>0 and len(pkl_files_list)==0:
                    #     print("images_files_list: ", images_files_list)
                    #     await files_images_handler.saving_files_to_ui(images_files_list)
                    
                    # if function_name=="insights_response" and len(images_files_list)==0 and len(pkl_files_list)>0:
                    #     print("pkl files list: ", pkl_files_list)
                    #     await files_images_handler.pkl_file_handler(pkl_files_list)

                    

                    # else:
                    #     print("No images created for this question....")
                
                    # if function_name == 'insights_response' and len(excel_files_list)>0:
                    #     print("excel_files_list: ", excel_files_list)
                    #     await files_images_handler.excel_file_handler(excel_files_list)


                    #         # try:
                    #         #     os.remove(filename)
                    #         #     print(f"{filename} has been deleted.")
                    #         # except Exception as e:
                    #         #     print(f"Error while deleting {filename}: {str(e)}")

                    # else:
                    #     print("No excel and ppt files created for this question....")
                    pass
                
                else:

                    if function_name=="python_after_sql" or function_name=="generate_sql_code" and len(images_files_list)>0 and len(pkl_files_list)==0:
                        print("images_files_list: ", images_files_list)
                        await files_images_handler.saving_files_to_ui(images_files_list)
                    
                    if function_name=="python_after_sql" or function_name=="generate_sql_code" and len(images_files_list)==0 and len(pkl_files_list)>0:
                        print("pkl files list: ", pkl_files_list)
                        await files_images_handler.pkl_file_handler(pkl_files_list)

                    

                    else:
                        print("No images created for this question....")
                
                    if function_name == 'python_after_sql' or function_name == 'generate_sql_code' and len(excel_files_list)>0:
                        print("excel_files_list: ", excel_files_list)
                        await files_images_handler.excel_file_handler(excel_files_list)


                            # try:
                            #     os.remove(filename)
                            #     print(f"{filename} has been deleted.")
                            # except Exception as e:
                            #     print(f"Error while deleting {filename}: {str(e)}")

                    else:
                        print("No excel and ppt files created for this question....")


                
                # if function_name=="create_867_sales_deck":
                #     print("inside refreshed ppt deck!!!!!!")
                #     # filename = "refreshed_ppt.pptx"
                #     excel_files_list_ = ["867_sales_refreshed_ppt.pptx"]
                #     saved_files_lst.append("867_sales_refreshed_ppt.pptx")
                #     # filename =
                #     for i, excel_file in enumerate(excel_files_list_):
                #         filename = excel_file
                #         if os.path.exists(filename):
                #             print("os path exists!!!")
                #             print("error while saving the file")
                #             time.sleep(1)
                #             #elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]
                #             elements = [cl.File(name= filename, path=f"./{filename}",display="inline",),]
                #             print("elements!!!!!!!!!!!!!!", elements)
                #             await cl.Message(content=f"File:{i+1}", elements=elements).send()#disable_feedback=False
                #             print("error while saving the file")
                #             time.sleep(1)
                #             if os.path.exists(filename):
                #                 try:
                #                     os.remove(filename)
                #                     print(f"{filename} has been deleted.")
                #                 except Exception as e:
                #                     print(f"Error while deleting {filename}: {str(e)}")
                #             else:
                #                 print(f"{filename} does not exist.")

                # if function_name=="create_claims_deck":
                #     print("inside refreshed ppt deck!!!!!!")
                #     # filename = "refreshed_ppt.pptx"
                #     excel_files_list_ = ["claims_refreshed_ppt.pptx"]
                #     saved_files_lst.append("claims_refreshed_ppt.pptx")
                #     # filename =
                #     for i, excel_file in enumerate(excel_files_list_):
                #         filename = excel_file
                #         if os.path.exists(filename):
                #             print("os path exists!!!")
                #             # print("error while saving the file")
                #             time.sleep(1)
                #             #elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]
                #             elements = [cl.File(name= filename, path=f"./{filename}",display="inline",),]
                #             await cl.Message(content=f"File:{i+1}", elements=elements).send()#disable_feedback=False
                #             # print("error while saving the file")
                #             time.sleep(1)
                #             if os.path.exists(filename):
                #                 try:
                #                     os.remove(filename)
                #                     print(f"{filename} has been deleted.")
                #                 except Exception as e:
                #                     print(f"Error while deleting {filename}: {str(e)}")
                #             else:
                #                 print(f"{filename} does not exist.")
            cur_iter+=1
            
        # Move to the next relevant question
        cnt += 1
        length.pop(0)
        if cl.user_session.get('chat_profile') in [BOT_TYPE_PMR,BOT_TYPE_MARKET_MAP,BOT_TYPE_EARLY_EXP,BOT_TYPE_PBC_MARKET_RESEARCH]:

                excel_files_list=cl.user_session.get('tool_params').get('excel_files_list',[])
                ppt_files_list=cl.user_session.get('tool_params').get('ppt_files_list',[])
                images_files_list=cl.user_session.get('tool_params').get('images_files_list',[])
                pkl_files_list=cl.user_session.get('tool_params').get('pkl_files_list',[])

                if len(images_files_list)>0 and len(pkl_files_list)==0:
                    print("images_files_list: ", images_files_list)
                    await files_images_handler.saving_files_to_ui(images_files_list)
                
                if len(images_files_list)==0 and len(pkl_files_list)>0:
                    print("pkl files list: ", pkl_files_list)
                    await files_images_handler.pkl_file_handler(pkl_files_list)

                else:
                    print("No images created for this question....")
            
                if len(excel_files_list)>0:
                    for elem in excel_files_list:
                        if 'segmented_analysis' in elem:
                            excel_files_list.remove(elem)
                    print("excel_files_list: ", excel_files_list)
                    await files_images_handler.excel_file_handler(excel_files_list)

                else:
                    print("No excel and ppt files created for this question....")
            


    output_files = copy.deepcopy(saved_files_lst)
    # insights_response, content_ui_message,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency = await sending_request_to_llm.insights_module(
    # data, user_message_copy, output_files,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency, chatbot_name
    # )


    # print("\n🔍 **Insights Response:**")
    # print(insights_response["content"])

    # stream_on_ui("generation of ppt is in progress please wait ...")
    # create_ppt(tool_params["result"], tool_params["python_code"], "output.pptx",insights=insights_response["content"])
    # saved_files_lst.append("output.pptx")
    # saved_files_lst.append("output_data.xlsx")
    # await files_images_handler.pptx_file_handler(["output.pptx"])
    # stream_on_ui("PPT generation completed successfully.")
    filename="_".join(stripes for stripes in cl.user_session.get("user_message").split()) if cl.user_session.get("user_message") else "output_deck"
    cl.user_session.set("llm_client",client)
    # cl.user_session.set("llm_client_model_id","onc_claude_sonnet_3-5")
    cl.user_session.set("llm_client_model_id",os.environ["model_id"])

    def sanitize_filename(filename):
        # Replace invalid characters with underscore
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Replace spaces with underscore
        filename = re.sub(r'\s+', '_', filename)
        # Replace multiple underscores with single
        filename = re.sub(r'_{2,}', '_', filename)
        # Remove leading/trailing underscores
        filename = filename.strip('_')
        return filename
    filename = sanitize_filename(filename)
    filename+=".pptx"
    # deck_params={'insights': insights_response["content"], 'python_code': tool_params["python_code"], 'result': tool_params["result"],"filename":filename}
    cl.user_session.set("deck_filename", filename)

    # await sending_request_to_llm.insights_module(user_message_copy, output_files)
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

    #cl.user_session.set("message_history", message_history)


clickable_handler = ClickableQuestionHandler(handle_user_query)

@cl.action_callback("Clickable_Question")
async def on_clickable_question(action: cl.Action):
   
    # databricks_token=cl.user_session.get("databricks_token")
    # Set the current thread ID for print statements
    update_thread_id_from_context()

    if cl.user_session.get("chat_profile") in [BOT_TYPE_PMR,BOT_TYPE_MARKET_MAP,BOT_TYPE_EARLY_EXP,BOT_TYPE_PBC_MARKET_RESEARCH]:
        query = "**"+str(action.label)+"**"
        print("the query from clickable question is pmr/market map: ", query)
        message_pmr=cl.Message(content=query)
        await run_conversation(message_pmr)

    else:

        databricks_auth_token=cl.user_session.get("databricks_auth_token")
        dataware_house=cl.user_session.get("dataware_house")
        if databricks_auth_token is None:
            redirect_url=providers[-1].redirect_uri
            domain=redirect_url[:redirect_url.find("/auth")]
            url=f"{domain}/auth/oauth/databricks"
            linked_text = f"❌ You're not authenticated. Please login [here]({url})."
            await cl.Message(content=linked_text).send()
        else:
            if dataware_house is None and databricks_auth_token:
                print("SQL Wareshouse Initialization")
                dataware_house=DataWarehouse(host_name,http_path,databricks_auth_token)
                cl.user_session.set("dataware_house",dataware_house)
                tool_params = cl.user_session.get("tool_params")
                tool_params['dataware_house']=dataware_house
                cl.user_session.set("tool_params",tool_params)
                print("SQL Wareshouse Initialized")
            await clickable_handler.process_clickable_question(action,dataware_house, cl.user_session.get("chatbot"))