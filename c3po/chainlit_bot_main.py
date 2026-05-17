import time
import json
import ast
import os
import chainlit as cl
import requests
import io
import tiktoken
import langchain
from langchain.tools.python.tool import PythonAstREPLTool
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
import copy
from typing import Dict, Optional

from Gilead.marketing_loader import MarketingDataLoader
from Gilead.gilead_loader import GileadDataLoader
from Gilead.ui_utils import UI_Utils
from Gilead.llm_requests_new  import LLM
from Gilead.helper import FileNameExtractor 
from Gilead.repl_tool_execution import PythonExecutionManager

# from Gilead.S3_code import LoadingFilesFromS3 
from Gilead.agent_tools import SalesPerformanceMetrics,DeckCreator
from Gilead.chatbot_manager import GileadBotManager
from Gilead.chat_session_manager import ChatSessionManager
from Gilead.chatbot_handler import ChatbotHandler 
from Gilead.sending_req_to_llm import RequestLLM
from Gilead.relevant_source_instructions import get_relevant_source_instructions_processor, SalesObject, ClaimsObject, DeckSourceHandler
from Gilead.handle_clickable_questions import ClickableQuestionHandler

from sql_warehouse_handling import DataWarehouse

client = AsyncOpenAI(
        api_key=os.getenv('DATABRICKS_TOKEN'),
        base_url=os.getenv("DATABRICKS_ENDPOINT_URL")
    )
model_id="claude_3-5_sonnet"

# import boto3
from botocore.exceptions import ClientError
import asyncio
from aiobotocore.session import get_session
session = get_session()

# bucket_name = 'gilead-c3po-input-data'  
# folder_name = 'c3po-3-jan/' 
# loader = LoadingFilesFromS3(bucket_name, folder_name)



request_llm =LLM()


marketing_loader = MarketingDataLoader()
gilead_loader = GileadDataLoader()


execution_manager = PythonExecutionManager(gilead_loader)
gilead_data = GileadDataLoader.load_sales_and_claims_data()
instructions = gilead_loader.load_instructions()
tools = gilead_loader.load_tools()
repl_tool = PythonExecutionManager(gilead_loader)

business_rules = instructions.get("business_rules", "")
claims_instructions = instructions.get("claims_instructions", "")
initial_prompt_gilead = instructions.get("initial_prompt_gilead", "")
agent_2_instructions = instructions.get("agent_2_instructions","")
code_instructions = instructions.get("code_instructions","")
revised_query_prompt = instructions.get("revised_query_prompt","")

functions_gilead = tools.get("functions_gilead","")



execution_manager = PythonExecutionManager(gilead_loader)
chatbot_handler = ChatbotHandler()
deck_creator = DeckCreator()
sending_request_to_llm = RequestLLM()

dw = DataWarehouse("dbc-5c6e6e7d-7beb.cloud.databricks.com", "/sql/1.0/warehouses/e1bbcab930429b81")
dw.get_data_from_wareshouse("select 1 as col")
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

# @cl.oauth_callback
# def oauth_callback(
#     provider_id: str,
#     token: str,
#     raw_user_data: Dict[str, str],
#     default_user: cl.User,
# ) -> Optional[cl.User]:
#     print(f"OAuth callback triggered for provider: {provider_id}")
#     print(f"User data: {raw_user_data}")

@cl.password_auth_callback
def auth_callback(username: str, password: str):# -> Optional[cl.AppUser]:
  # Fetch the user matching username and compare the password with the value stored in the database
  usernames_passwords_lst = [('husna.siddiqa@setuserv.com', 'setuserv123'), ('kalyankumar.marella@setuserv.com', 'setuserv123'), ('sanga', 'setuserv123'), ('sanjeev.kayath@gilead.com', 'gilead123'), ('gaurav.bhatnagar@gilead.com', 'gilead123'), ('sadhna.thakur5@gilead.com', 'gilead123'), ('Badari.Ganti@gilead.com', 'gilead123'), ('karthik.jodu@setuserv.com', 'setuserv123'), ('shashikumar.vemula@setuserv.com','setuserv123'),('samvidha.reddy@setuserv.com', 'setuserv123'), ('parashuram.reddy@setuserv.com','setuserv123'), ('Sumit.Singh70@gilead.com', 'gilead123'), ('sanga@setuserv.com', 'setuserv123'), ('srikanth.reddy@setuserv.com', 'setuserv123'), ('kristi.pedersen@gilead.com', 'gilead123'), ('SangaReddy.Peerreddy@gilead.com', 'gilead123'), ('diego.dimes@gilead.com', 'gilead123'), ('samvidha.reddy@setuserv.com', 'setuserv123'),('sayantan.dasgupta@arvinas.com','arvinas123'),('yshankar@prescriptiveinsights.com','prescriptive123'),('suresh.divakar@prescriptiveinsights.com','prescriptive123'),('avani.patlolla@setuserv.com','setuserv123'),('chandana.rajarapu@setuserv.com','setuserv123'),('vikram@multiplierai.com','multiplierai123'),('guest@setuserv.com','setuserv123'),('nalini.purkayastha@gilead.com','gilead123'),('shomita.mandal@gilead.com','gilead123'),('pete.bielecki@gilead.com','gilead123'),('carolynn.chang@gilead.com','gilead123'),('nadia.cole@gilead.com','gilead123')]
  new_lst = []
  for tup in usernames_passwords_lst:
      new_lst.append((tup[0].lower(), tup[1].lower()))
  if (username.lower(), password.lower()) in new_lst:
    #cl.user_session.set("username", username)
    return cl.User(identifier=username, metadata={"role": "admin", "provider": "credentials"})
  else:
    return None
   
    result = loader.load_files_from_s3()
    refresh_data(result)

    return default_user


print("loading file into python env at 130 line...., i.e loading code is running.....")


# # Run the async function in an event loop
asyncio.run(execution_manager.run_loading_code())



@cl.step

@cl.on_chat_start
async def start():
    await chatbot_handler.start_chat()

    print("inside start_Chart/............")



@cl.on_settings_update
async def update_settings(settings):
    await chatbot_handler.setup_agent(settings)
    

@cl.on_message
async def run_conversation(message: cl.Message):
    """
    Handles the user's input message, revises the query if necessary,
    and processes it based on the active chatbot's context (e.g., Gilead).
    """
    #Extract the user's query from the message
    query = message.content

    # Retrieve the message history from the user session
    message_history = cl.user_session.get("message_history")
    print("message_history!!!!!!!!!!!!!!",len(message_history),  message_history)

    # Retrieve the active chatbot name from the session
    chatbot_name = cl.user_session.get("chatbot")
    print("chatbot_name!!!!!!!!!!!", chatbot_name)

    # Check if the active chatbot is "Gilead"
    if chatbot_name == "Gilead":
        print("in chatbot_name !!!")
        # If there is a message history, revise the query
        if len(message_history) > 1:
            revised_query = await sending_request_to_llm.get_revised_query(revised_query_prompt, message_history, query)
        else:
            revised_query = query # No revision needed for the first query
        print("The query is!!!!!!!!!!!", query)
        print("revised_query!!!!!!!!!!!!!", revised_query)

        # Send the revised query back to the UI for reference
        rev_query =cl.Message(content="Revised query: "+revised_query, disable_feedback=True)
        await rev_query.send()
        print("This is the revised query",revised_query)

        data_source_dict = await sending_request_to_llm.ask_bedrock_claude(revised_query)
        
        # Determine the data source and fields using Bedrock Claude
        data_source, fields, gilead_prompt = await get_relevant_source_instructions_processor(data_source_dict)
        print("Extracted Fields:", fields)

      
        # Process the revised query with the appropriate data source and prompt
        await handle_user_query(revised_query, data_source, fields=fields, gilead_prompt=gilead_prompt)
    else:
        # pass
        await handle_user_query(query,data_source='', fields=fields, gilead_prompt='' )


# relevent_business_rules = await sending_request_to_llm.get_relevent_business_rules(gilead_prompt, user_message_copy)

async def handle_user_query(user_message, data_source='', fields=[],  gilead_prompt=''):

    email = cl.user_session.get("user").identifier
    user_message_copy = user_message
    if "business" in user_message.lower():
        user_message += ''' To calculate Source of Business Shares:
         - Use occurance(frequency) of each drug/ total occurances(frequency) of each drug. '''
    # user_message = user_message + '''Common Procedures for claims data:
    # - Use year_month to extract year/quarter for using in yearly/ Quartelry analysis'''
    # user_message = user_message + " If the answer is already present in the query, just give out that answer instead of calculating again. If you plot a chart, Save the table behind the chart into a CSV/XLSX file"
    user_message = user_message + " If the answer is an information provided rather than a question i.e already answer is present in the query, just give out that answer instead of calculating again. If indication is mentioned, filter for the indication in all cases. If you plot a chart, Save the table behind the chart into a CSV/XLSX file. Please note you will not have access to existing CSV/PPT files to answer for follow up questions. In that case you need to calculate again"

    print("type:  ", type(cl.user_session))

    chatbot_name = cl.user_session.get("chatbot")
    message_history = cl.user_session.get("message_history")
    print("gilead_prompt structure:", gilead_prompt)
    relevent_business_rules = await sending_request_to_llm.get_relevent_business_rules(gilead_prompt, user_message_copy,fields)
    print("the relevant business rules picked are: ",relevent_business_rules)
    system_prompt = agent_2_instructions.format(fields = fields, code_instructions = code_instructions, business_rules = relevent_business_rules)
    print('total system prompt in messages history for the question :',system_prompt)


    if chatbot_name == "Gilead":
        # gilead_prompt += 'Foremost step is to tell which data source you are'
        message_history[0] = {"role": "system", "content": system_prompt}

    message_history = cl.user_session.get("message_history")
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
    while cur_iter < MAX_ITER:

        # OpenAI call
        claude_message = {"role": "", "content": ""}
        function_ui_message = None
        content_ui_message = cl.Message(content="")
        async with cl.Step(name = "C3PO", type = "llm", root = True, disable_feedback = False) as content_ui_message:
            async with cl.Step(name="Python Code", type="llm", root=False, show_input= False, language = "python") as function_ui_message:
                active_bot = chatbot_name
                active_bot_functions = functions_gilead if active_bot == "Gilead" else functions_marketing

                print(f"current_history of {cur_iter}: ",current_history)
            # Use async for loop to iterate over the response
                response = await request_llm.send_request_streaming(current_history,tools=active_bot_functions)

                # print("RESPONSE : ",response)
                async for stream_resp in response:
                    # print("stream_response = ",stream_resp.choices[0].finish_reason)
                    new_delta = stream_resp.choices[0].delta
                    # print("new_delta=",new_delta)
                    claude_message, content_ui_message, function_ui_message = await UI_Utils.process_new_delta(
                        new_delta, claude_message, content_ui_message, function_ui_message)
                    # print(chat_completion.choices[0].message.content)

                # Run the async function

                message_history.append(claude_message)
                current_history.append(claude_message)
            print("claude_message at 583: ", claude_message)
            # if function_ui_message is not None:
            #     await function_ui_message.send()
            print(stream_resp.choices[0])
            if stream_resp.choices[0].finish_reason == "stop":
                break
            elif stream_resp.choices[0].finish_reason != "tool_calls":
                raise ValueError(stream_resp.choices[0].finish_reason)
            # Safely extract function call details (if they exist)
            function_name = claude_message.get("tool_calls", [{}])[0].get("function", {}).get("name", "")
            # print("RETRIVING FUNCTION NAME : ",function_name)

            # Fetch the tool_call_id from the function_call, assuming the function_call exists
            tool_call_id = claude_message.get("tool_calls", {})[0].get("id", "")
            # print("RETRIVING id  : ",tool_call_id)

            # Modify the arguments to exclude the first two characters as per the requirement
            argument_str = claude_message.get("tool_calls", [{}])[0].get("function", {}).get("arguments", "")
            # print("ARGUMENT STRING : ",argument_str)
            if function_name=="run_python_code" or  function_name == "run_python" or function_name=="calculate_date_ranges":
                arguments = ast.literal_eval(argument_str)
            else:
                arguments = ast.literal_eval(argument_str)
            if function_name == "calculate_date_ranges"  :
                python_ui_message = function_name + argument_str
            else:
                python_ui_message = arguments.get("query")
            # await function_ui_message.stream_token(python_ui_message)
            function_ui_message.output = python_ui_message
            await function_ui_message.update()
            await cl.sleep(0.1)
            if function_name=='run_python_code' or  function_name == "run_python":
                try:
                    before_current_time = datetime.now()  # Get the current datetime before the await
                    print('in code excution')
                    code = arguments.get("query")
                    print('code = ',code)
                    # function_response = await cl.make_async(repl_tool_fun)(code)  # Your asynchronous function call
                    function_response = await execution_manager.run_python_execution(code)
                    # code = arguments.get("query")
                 
                    after_current_time = datetime.now()  # Get the current datetime after the await

                    print("before await!!!!!!")
                    # Calculate the time difference as a timedelta
                    time_difference = after_current_time - before_current_time
                    print("time difference !!!!!!!!", time_difference)
                    await cl.sleep(1)

                except:
                    print("in exception!!!!")
                    code = arguments.get("query")
                    # function_response = await repl_tool_fun(arguments.get("query"))
                    function_response = await execution_manager.run_python_execution(code)
                    print("before await 2!!!!!!!!!")
                    await cl.sleep(0.5)


            elif function_name == 'create_867_sales_deck':
                #status = await cl.make_async(deck_creator.create_867_sales_deck)()
                status = await deck_creator.create_867_sales_deck()
                print("subprcoess python file execution statues: ", status)
                print("function called")

                if status == True:
                    function_response = "Successfully Deck created"
                else:
                    function_response = "Failed to create Deck"

            elif function_name == 'create_claims_deck' :
                status = await deck_creator.create_claims_deck()
                print("subprcoess python file execution statues: ", status)
                print("function called")

                # function_response = repl_tool_fun2(download_ppt_code)
                if status == True:
                    function_response = "Successfully Deck created"
                else:
                    function_response = "Failed to create Deck"



            elif function_name == "calculate_date_ranges":
                print("inside calculate_date_ranges called....")
                function_response = "R13W value date ranges: start_date - 01/01/2024 and end_date - 19/04/2024"
                formatted_latest_weekend_date = arguments.get("query")
                date_ranges = SalesPerformanceMetrics.calculate_date_ranges(formatted_latest_weekend_date)

                m12_dates_dic = {
                    "r12m_start_date": date_ranges["r12m_start_date"],
                    "r12m_end_date": date_ranges["r12m_end_date"],
                    "p12m_start_date": date_ranges["p12m_start_date"],
                    "p12m_end_date": date_ranges["p12m_end_date"],
                }

                w4_dates_dic = {
                    "start_r4w": date_ranges["start_r4w"],
                    "end_r4w": date_ranges["end_r4w"],
                    "start_p4w": date_ranges["start_p4w"],
                    "end_p4w": date_ranges["end_p4w"],
                }

                w13_dates_dic = {
                    "start_r13w": date_ranges["start_r13w"],
                    "end_r13w": date_ranges["end_r13w"],
                    "start_p13w": date_ranges["start_p13w"],
                    "end_p13w": date_ranges["end_p13w"],
                }

                qtd_dates_dic = {key: value for key, value in date_ranges.items() if key.startswith("start_q") or key.startswith("end_q")}

                function_response = f"""Here are the different Date Ranges for different metrics:
                {m12_dates_dic} \n
                {w4_dates_dic} \n
                {w13_dates_dic} \n
                {qtd_dates_dic} \n

                Remember that R1M(Recent 1 month) is same as R4W and P1M is similar to P4W
                AND R3M is similar to R13M and P3M is similar to P13M and Q1TD(quarter 1 to TO(latest) Date) is same as Q2TD,Q3TD,Q4TD.
                """
                print("function_response at calculate_date_ranges method: ", function_response)

#
            # else:
            #     print("function is not being called")




# {'tool_call_id': 'call_hhKpjAtgSxgfWgh1BK1Jcizr', 'role': 'tool', 'name': 'fetch_listing_data', 'content': {'foo': 'bar'}}]


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



            # await cl.Message(
            #     author="Python Code Execution Output",
            #     content = "\n\nAnswer: \n"+str(function_response),
            #     language = "json",
            # ).send()
            if function_name == 'run_python_code':
                async with cl.Step(name="Python Code Execution Output", type="llm", root=False, show_input= False, language = "python") as python_code_exc_output:
                    print("in output execution !!!!!!!")
                    python_code_exc_output.output = function_response
                    print("after python_code_exc_output !!!!!!")
                    # await python_code_exc_output.update() 
                    await cl.sleep(0.1)
                python_code = arguments.get("query")
                images_files_list = FileNameExtractor.extract_images_names(python_code)
                excel_files_list = FileNameExtractor.extract_excel_names(python_code)

                ppt_files_list = FileNameExtractor.extract_ppt_names(python_code)

                excel_files_list.extend(ppt_files_list)

                saved_files_lst.extend(images_files_list)
                saved_files_lst.extend(excel_files_list)



            if function_name == 'run_python_code' and len(images_files_list)>0:
                print("images_files_list: ", images_files_list)
                for i, image_file in enumerate(images_files_list):
                    filename = image_file
                    if os.path.exists(filename):
                        elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]

                        await cl.Message(content=f"Chart:{i+1}", elements=elements,disable_feedback=False).send()

                        await cl.sleep(1)
                        if os.path.exists(filename):
                            try:
                                os.remove(filename)
                                print(f"{filename} has been deleted.")
                            except Exception as e:
                                print(f"Error while deleting {filename}: {str(e)}")
                        else:
                            print(f"{filename} does not exist.")

            else:
                print("No images created for this question....")

            if function_name == 'run_python_code' and len(excel_files_list)>0:
                print("excel_files_list: ", excel_files_list)
                for i, excel_file in enumerate(excel_files_list):
                    filename = excel_file
                    if os.path.exists(filename):
                        #elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]
                        elements = [cl.File(name= filename, path=f"./{filename}",display="inline",),]

                        try:
                            await cl.Message(content=f"File-{i+1}: {excel_file}", elements=elements).send()
                            await cl.sleep(0.5)
                        except Exception as e:
                            print("Exception occured at 907 and e: ", e)


                        # try:
                        #     os.remove(filename)
                        #     print(f"{filename} has been deleted.")
                        # except Exception as e:
                        #     print(f"Error while deleting {filename}: {str(e)}")

            else:
                print("No excel and ppt files created for this question....")


            if function_name=="create_867_sales_deck":
                print("inside refreshed ppt deck!!!!!!")
                # filename = "refreshed_ppt.pptx"
                excel_files_list_ = ["867_sales_refreshed_ppt.pptx"]
                saved_files_lst.append("867_sales_refreshed_ppt.pptx")
                # filename =
                for i, excel_file in enumerate(excel_files_list_):
                    filename = excel_file
                    if os.path.exists(filename):
                        print("os path exists!!!")
                        print("error while saving the file")
                        time.sleep(1)
                        #elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]
                        elements = [cl.File(name= filename, path=f"./{filename}",display="inline",),]
                        print("elements!!!!!!!!!!!!!!", elements)
                        await cl.Message(content=f"File:{i+1}", elements=elements,disable_feedback=False).send()
                        print("error while saving the file")
                        time.sleep(1)
                        if os.path.exists(filename):
                            try:
                                os.remove(filename)
                                print(f"{filename} has been deleted.")
                            except Exception as e:
                                print(f"Error while deleting {filename}: {str(e)}")
                        else:
                            print(f"{filename} does not exist.")

            if function_name=="create_claims_deck":
                print("inside refreshed ppt deck!!!!!!")
                # filename = "refreshed_ppt.pptx"
                excel_files_list_ = ["claims_refreshed_ppt.pptx"]
                saved_files_lst.append("claims_refreshed_ppt.pptx")
                # filename =
                for i, excel_file in enumerate(excel_files_list_):
                    filename = excel_file
                    if os.path.exists(filename):
                        print("os path exists!!!")
                        # print("error while saving the file")
                        time.sleep(1)
                        #elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]
                        elements = [cl.File(name= filename, path=f"./{filename}",display="inline",),]
                        await cl.Message(content=f"File:{i+1}", elements=elements,disable_feedback=False).send()
                        # print("error while saving the file")
                        time.sleep(1)
                        if os.path.exists(filename):
                            try:
                                os.remove(filename)
                                print(f"{filename} has been deleted.")
                            except Exception as e:
                                print(f"Error while deleting {filename}: {str(e)}")
                        else:
                            print(f"{filename} does not exist.")


    output_files = copy.deepcopy(saved_files_lst)
    insights_response, content_ui_message = await sending_request_to_llm.insights_module(
    user_message_copy, output_files
    )

    print("\n🔍 **Insights Response:**")
    print(insights_response["content"])


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
    #cl.user_session.set("message_history", message_history)

clickable_handler = ClickableQuestionHandler(handle_user_query=handle_user_query)


@cl.action_callback("Clickable_Question")
async def on_clickable_question(action: cl.Action):
    await clickable_handler.process_clickable_question(action)
