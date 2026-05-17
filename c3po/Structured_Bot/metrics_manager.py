#Manages OpenTelemetry metrics collection
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
import re
from datetime import datetime, timedelta
from openai import OpenAI,AsyncOpenAI
import asyncio
from dotenv import load_dotenv
import copy
from typing import Dict, Any, Optional
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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from chainlit.element import ElementDict
from chainlit.logger import logger
from chainlit.types import Feedback
from dataclasses import asdict

class MetricsManager:
    def __init__(self):
        self.total_tokens_conv = 0
        self.input_tokens_conv = 0
        self.output_tokens_conv = 0
        self.latency = 0
        self.setup_metrics()
    
    def setup_metrics(self):
        """
        Initialize OpenTelemetry metrics
        """
        self.meter = get_meter(__name__)
        self.total_tokens_metric = self.meter.create_counter(
            name="Total_Tokens", 
            description="Counter for LLM token usage", 
            unit="1"
        )
        self.total_tokens_cost = self.meter.create_counter(
            name="Total_Tokens_Usage_Cost",
            description="Counter for LLM token usage cost",
            unit="1"
        )
        self.latency_metric = self.meter.create_histogram(
            name="Latency", 
            description="Latency of LLM's",
            unit="1"
        )
        self.tokens_per_conv = self.meter.create_counter(
            name="Tokens_per_conversation",
            description="Tokens per conversation",
            unit="1"
        )
        self.response_time_metric = self.meter.create_counter(
            name="Response_Time",
            description="Response time",
            unit="1"
        )
    
    def update_metrics(self, tokens_data=None, latency_value=None, response_time=None):
        """
        Update metrics with new values
        
        Args:
            tokens_data: Dict containing 'total', 'input', and 'output' token counts
            latency_value: LLM response latency value
            response_time: Total response time for request
        """
        if tokens_data:
            self.total_tokens_conv = tokens_data.get('total', self.total_tokens_conv)
            self.input_tokens_conv = tokens_data.get('input', self.input_tokens_conv)
            self.output_tokens_conv = tokens_data.get('output', self.output_tokens_conv)
        
        if latency_value is not None:
            self.latency = latency_value
            
        if response_time is not None:
            self.response_time_metric.add(response_time)
    
    def record_final_metrics(self):
        """
        Calculate and record final metrics for the conversation
        """
        input_token_usage_cost = round(self.input_tokens_conv * 0.000003, 2)
        output_token_usage_cost = round(self.output_tokens_conv * 0.000015, 2)
        total_token_usage_cost = round(input_token_usage_cost + output_token_usage_cost, 2)
        
        self.total_tokens_metric.add(self.total_tokens_conv)
        self.total_tokens_cost.add(total_token_usage_cost)
        self.latency_metric.record(round(self.latency, 2))
        self.tokens_per_conv.add(round(self.total_tokens_conv / 6, 2))
        
        return {
            'input_tokens': self.input_tokens_conv,
            'output_tokens': self.output_tokens_conv,
            'total_tokens': self.total_tokens_conv,
            'cost': total_token_usage_cost
        }