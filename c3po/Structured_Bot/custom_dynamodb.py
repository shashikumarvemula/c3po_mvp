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
import copy
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
from typing import Dict, Any, Optional
from traceloop.sdk.decorators import workflow, task
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from chainlit.element import ElementDict
from chainlit.logger import logger
from chainlit.types import Feedback
from dataclasses import asdict
import matplotlib
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
# Metric imports
from opentelemetry.metrics import get_meter
from opentelemetry import metrics as metrics
from Structured_Bot.S3_code import LoadingFilesFromS3 

logger = logging.getLogger(__name__)

class CustomDynamoDBDataLayer(DynamoDBDataLayer):
    def __init__(self, table_name, storage_provider, user_thread_limit=50):
        # Create a DynamoDB client
        session = boto3.Session()
        dynamodb = session.resource('dynamodb', region_name='us-west-2')
        self.dynamodb_client = session.client('dynamodb', region_name='us-west-2')
        
        # Initialize the parent class with the DynamoDB client
        super().__init__(table_name, self.dynamodb_client, user_thread_limit)
        
        # Store the S3 storage provider
        self.storage_provider = storage_provider
        self.s3_client = boto3.client('s3')
    
    def generate_presigned_url(self, bucket_name, object_key, expiration=3600):
        try:
            url = self.s3_client.generate_presigned_url('get_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=expiration)
            return url
        except Exception as e:
            print(f"Error generating pre-signed URL: {e}")
            return None

    def _deserialize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # print("checking signed url:", item)
        val = 0
        url_value = ""
        sample = {}
        
        for key, value in item.items():
            if key == "url":
                print("Key Value : ", value)
                index = value['S'].find("s3.amazonaws.com")
                index = index + len("s3.amazonaws.com")
                object_key = value['S'][index+1:]
                first_index = value['S'].find("https://")
                first_index = first_index + len("https://")
                last_index = value['S'].find("s3.amazonaws.com")
                bucket_name = value['S'][first_index:last_index-1]
                value['S'] = self.generate_presigned_url(bucket_name, object_key)
                
            sample = {key: self._type_deserializer.deserialize(value)}
            val += 1
            
        return {
            key: self._type_deserializer.deserialize(value)
            for key, value in item.items()
        }
    
    async def upsert_feedback(self, feedback: Feedback) -> str:
        logger.info(
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
        
        return feedback.id