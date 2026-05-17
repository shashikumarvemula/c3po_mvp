from Structured_Bot.llm_requests_new import LLM
import chainlit as cl
from gilead_loader import GileadDataLoader
from Structured_Bot.ui_utils import UI_Utils
import pandas as pd
import json
import time
from traceloop.sdk.decorators import task
import datetime
import ast

request_llm = LLM()
# instructions = GileadDataLoader.load_instructions()
# print("instructions at gilead loader import : ",instructions.keys())
# descriptions = GileadDataLoader.load_tools()
# configs = GileadDataLoader.load_configs()
# configs = configs.get("configs", "")
# list_of_data_sources = configs["list_of_data_sources"]

# if len(list_of_data_sources)!=1:
#     get_data_source_prompt = instructions["get_data_source"]
# else:
#     get_data_source_prompt = ""

class RequestLLM:
    @staticmethod
    async def get_data_source_deck(user_question_content,system_content):
        try:
            # Make a request to the Claude model
            total_tokens_conv=0
            input_tokens_conv=0
            output_tokens_conv=0
            email=cl.user_session.get("user").identifier
            latency=0
            start_time=time.time()
            chat_completion ,email,total_tokens_conv,input_tokens_conv,output_tokens_conv = await request_llm.send_request_non_streaming(
                system_content=system_content, user_content=user_question_content,email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv
            )
            end_time=time.time()
            latency+=(end_time-start_time)
            print("Claude's response:", chat_completion)
            print("in data source tokens",total_tokens_conv)
            return chat_completion # Return the full response after streaming  # Return the full response after streaming
        except Exception as e:
            # Handle and log any errors that occur during the API call
            print(f"ERROR: Can't invoke 'onc_claude_sonnet_3-5' in ask_bedrock_claude. Reason: {e}")
            return None
    @task("getting relevent fields and data source")
    async def get_data_source_and_fields(self,user_question_content,email, total_tokens_conv, input_tokens_conv, output_tokens_conv,latency, *, system_content):
        try:
            # Make a request to the Claude model
            print("Get data source prompt at 50: ",system_content)
            print("User question at line 50: ",user_question_content)
            start_time=time.time()
            chat_completion ,email,total_tokens_conv,input_tokens_conv,output_tokens_conv = await request_llm.send_request_non_streaming(
                system_content=system_content, user_content=user_question_content,email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv
            )
            end_time=time.time()
            latency+=(end_time-start_time)
            print("Claude's response:", chat_completion)
            print("in data source tokens",total_tokens_conv)
            return chat_completion ,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency # Return the full response after streaming  # Return the full response after streaming
        except Exception as e:
            # Handle and log any errors that occur during the API call
            print(f"ERROR: Can't invoke 'onc_claude_sonnet_3-5' in ask_bedrock_claude. Reason: {e}")
            return None

    @staticmethod
    @task("generating revised query")
    async def get_revised_query(revised_query_prompt,message_history, query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency):

        revised_query_prompt = revised_query_prompt.format(
            message_history=message_history, query=query
        )
        print("Length of revised query prompt:", len(revised_query_prompt.split()))
        print("Message history in get_revised_query:", message_history)
        try:
            # Make a request to Claude model
            start_time = time.time()
            chat_completion ,email,total_tokens_conv,input_tokens_conv,output_tokens_conv = await request_llm.send_request_non_streaming(
                system_content=revised_query_prompt, user_content=query,email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv
            )
            end_time=time.time()
            latency =latency+(end_time-start_time)
            # Extract the response text from the model's output
            print("in revised query tokens",total_tokens_conv)
            # Extract the response text from the model's output
            print("chat completion",chat_completion)
            sanitized_response = chat_completion.replace("\n", " ").replace("\r", " ")
            data = json.loads(sanitized_response)

            full_text = data['revised_query']

            # Extract the question part (assumed to end at the first '?')
            if '?' in full_text:
                question_end_idx = full_text.find('?') + 1
                question = full_text[:question_end_idx]
            else:
                question=full_text

            print("here is the revised question",question)
            revised_query=question
            # output_response = chat_completion["revised_query"]
            # print("Output response from Claude:", output_response)
            # revised_query = eval(output_response)["revised_query"]
            return revised_query,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
            # Optionally, log the full chat_completion to debug the invalid characters
            return query, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
        except Exception as e:
            print(f"ERROR: Can't invoke 'get_revised_query'. Reason: {e}")
            return query,email, total_tokens_conv, input_tokens_conv, output_tokens_conv,latency

    @staticmethod   
    @task("picking relevent example queries") 
    async def pick_relevant_example_queries(relevant_query_picking_agent_prompt,user_question_content,queries_code,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency):
        # Do not format the prompt again here. It may contain brace-wrapped
        # survey placeholders like {EQ_APR_NR.shown} which should remain literal.
        # Formatting again would treat them as Python format fields and raise KeyError.
        system_prompt = relevant_query_picking_agent_prompt
        print("the prompt for picking example queries is:",system_prompt )
        try:
            # Make a request to the Claude model
            start_time=time.time()
            chat_completion,email,total_tokens_conv,input_tokens_conv,output_tokens_conv = await request_llm.send_request_non_streaming(
                system_content=system_prompt, user_content=user_question_content,email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv
            )
            end_time=time.time()
            latency+=(end_time-start_time)
            print("in example tokens",total_tokens_conv)
            print(f"Relevant example queries and code selected for question: {user_question_content} ->", chat_completion)
            # return chat_completion  # Return the full response after streaming
            return chat_completion,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency # Return the full response after streaming
        except Exception as e:
            # Handle and log any errors that occur during the API call
            print(f"ERROR: Can't invoke 'onc_claude_sonnet_3-5' in pick_relevant_example_queries. Reason: {e}")
            return None


    @staticmethod
    @task("getting the relevent business rules")
    async def get_relevant_business_rules(get_relevant_business_rules_prompt, all_business_rules, user_query_relevant_rules,fields,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency):
        print(f"all_business_rules type: {type(all_business_rules)}")
        print(f"all_business_rules content: {all_business_rules}")
        print(f"fields type: {type(fields)}")
        print(f"fields content: {fields}")
        agent1_instructions = get_relevant_business_rules_prompt
        system_prompt = agent1_instructions.format(all_business_rules=all_business_rules,fields=fields)
        print("get_relevant_rules prompt:", system_prompt)
        try:
            # Make a request to Claude model
            start_time=time.time()
            response_rules,email,total_tokens_conv,input_tokens_conv,output_tokens_conv = await request_llm.send_request_non_streaming(
                system_content=system_prompt, user_content=user_query_relevant_rules,email=email,total_tokens_conv=total_tokens_conv,input_tokens_conv=input_tokens_conv,output_tokens_conv=output_tokens_conv
            )
            end_time=time.time()
            latency+=(end_time-start_time)
            print("in business rules tokens",total_tokens_conv)
            print(f"Output response from Claude in relevant business rules function: for question {user_query_relevant_rules} ->", response_rules)
            return response_rules ,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency
        except Exception as e:
            print(f"ERROR: Can't invoke 'get_relevant_business_rules'. Reason: {e}")
            return None

    @staticmethod
    @task("Generating insights from the output")
    async def insights_module(data, user_message_copy, saved_files_lst,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency, chatbot_name):
        claude_message = {"role": "", "content": ""}
        content_ui_message = cl.Message(content="")
        function_ui_message = None
        print("saved files list",saved_files_lst)

        data_list = []
        for file in saved_files_lst:
            try:
                if file.endswith('.csv'):
                    df = pd.read_csv(file)
                elif file.endswith('.xlsx') or file.endswith('.xls'):
                    df = pd.read_excel(file)
                else:
                    continue
                data_list.append({
                    "File Name": file,
                    "File's Content": df.to_dict(orient='records')
                })
            except Exception as e:
                print(f"Error processing file {file}: {e}")

        json_data_combined = json.dumps(data_list, indent=4)
        list_data = json.loads(json_data_combined)
        print("json_data_combined (Python Object):", json_data_combined)
        # Function to check if the first key in the first dictionary has a value
        def check_if_data_exist(data):
            try:
                # Validate if data is a non-empty list
                if not isinstance(data, list) or not data:
                    print(f"Invalid Data Structure (Not a list or empty): {data}")
                    return False
                file_entry = data[0]  # Get the first dictionary entry
                if not isinstance(file_entry, dict):
                    print(f"Invalid Data Structure (First entry not a dictionary): {file_entry}")
                    return False
                # Ensure "File's Content" exists, is a list, and is non-empty
                file_content = file_entry.get("File's Content")
                if not isinstance(file_content, list) or not file_content:
                    print("Missing or empty 'File's Content'")
                    return False
                first_dict = file_content[0]  # Get the first dictionary inside "File's Content"
                if not isinstance(first_dict, dict) or not first_dict:
                    print("First dictionary inside 'File's Content' is empty or not a dictionary")
                    return False
                # Get the first key in the dictionary
                first_key = next(iter(first_dict), None)
                if first_key is None:  # If there's no key, return False
                    print("No key found in the first dictionary")
                    return False
                first_value = first_dict.get(first_key)
                print(f"First Key: {first_key}, First Value: {first_value}")
                # Return True if the first key exists and has a non-empty, non-null value
                return bool(first_value)
            except Exception as e:
                print(f"Error while checking JSON structure: {e}")
                return False
        # Measure execution time
        start_time = time.time()
        # Run the function with corrected data
        result = check_if_data_exist(list_data)
        # Print result
        insights_resp={}
        print(f"Result for JSON Data: {result}")
        if not result:
            print("Json Data Does not Exist")
            insights_resp["content"]=""
            return insights_resp,'',email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency
        else:
            print("Json Data Exists")
        print(f"Execution Time For Insights: {time.time() - start_time}")
        # Parse the JSON string back into a Python object
        parsed_data = json.loads(json_data_combined)
        json_data_combined = [file_data["File's Content"] for file_data in parsed_data]

        print(f"Json Data Combined at 1503: {json_data_combined}")
        print("Combined Data in JSON Format:")
        print(json.dumps(json_data_combined, indent=4))  # Convert back to JSON string for display
        print(f'User_Query at 2152: {user_message_copy}')
        print(f'Output Files at 2155: {saved_files_lst}')
        print(f'Json Data at 2192: {json.dumps(json_data_combined, indent=4)}')
        print(f'Length of Json Data: {len(json_data_combined)}')

        insights_header = "Key Insights: " + '\n' * 5
        current_timestamp = datetime.datetime.now()
        system_prompt = data[chatbot_name]['instructions']["insights_module_instructions"].format(insights_header=insights_header, date=current_timestamp)

        current_history = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"User question: {user_message_copy}\nData: {str(json_data_combined)}"}
        ]

        try:
            start_time=time.time()
            response = await request_llm.send_request_streaming(current_history)
            end_time=time.time()
            latency+=(end_time-start_time)
            async for stream_resp in response:
                if stream_resp.choices[0].delta:
                    new_delta = stream_resp.choices[0].delta
                    claude_message, content_ui_message, function_ui_message,stream_resp = await UI_Utils.process_new_delta(
                        new_delta, claude_message, content_ui_message, function_ui_message,stream_resp,True
                    )
                    if(stream_resp.usage.prompt_tokens!=None and stream_resp.usage.completion_tokens!=None):
                        prompt_tokens = stream_resp.usage.prompt_tokens
                        completion_tokens = stream_resp.usage.completion_tokens
                        total_tokens = prompt_tokens + completion_tokens
                        input_tokens_conv+=prompt_tokens 
                        output_tokens_conv+=completion_tokens
                        total_tokens_conv+=total_tokens
                        print("in insights tokens",total_tokens)
                        print("conv input",input_tokens_conv,"conv output", output_tokens_conv,"total conv",total_tokens_conv)
                if stream_resp.choices[0].finish_reason == 'stop':
                    break
            # return claude_message, content_ui_message
            return claude_message, content_ui_message,email,total_tokens_conv,input_tokens_conv,output_tokens_conv,latency
        except Exception as e:
            print(f"Error in insights generation: {e}")
            return {"role": "assistant", "content": str(e)}, None

    @staticmethod
    @task("code generation and analysis plan")
    async def analysis_plan_and_code_generating_agent(content_ui_message, claude_message, stream_resp_sql, function_ui_message, current_history, tools, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency):
        # claude_message = {"role": "", "content": ""}
        # content_ui_message = cl.Message(content="")
        # function_ui_message = None

        # try:
        print("Starting code generation analysis plan...")
        start_time = time.time()
        response = await request_llm.send_request_streaming(current_history, tools=tools)
        end_time = time.time()
        latency += (end_time-start_time)

        print("Processing streaming response...")
        async for stream_resp in response:
            if stream_resp.choices[0].delta:
                new_delta = stream_resp.choices[0].delta
                print("New delta received:", new_delta)  # Important for debugging
                claude_message, content_ui_message, function_ui_message, stream_resp = await UI_Utils.process_new_delta(
                    new_delta, claude_message, content_ui_message, function_ui_message, stream_resp, True)
                
                if(stream_resp.usage.prompt_tokens!=None and stream_resp.usage.completion_tokens!=None):
                    prompt_tokens = stream_resp.usage.prompt_tokens
                    completion_tokens = stream_resp.usage.completion_tokens
                    total_tokens = prompt_tokens + completion_tokens
                    input_tokens_conv += prompt_tokens 
                    output_tokens_conv += completion_tokens
                    total_tokens_conv += total_tokens
                    print("first call conv",total_tokens)
                    print("conv input",input_tokens_conv,"conv output", output_tokens_conv,"total conv before code check",total_tokens_conv)
                    print("Token usage in code generation:", total_tokens)

            print("the function_ui_message is before code checking", function_ui_message)  
            print("claude_message before sending to code checking agent is: ", claude_message)
            print(f"Stream Response Choice: {stream_resp.choices[0]}")
            # if stream_resp.choices[0].finish_reason == 'stop':
            #     print("Code generation completed with finish_reason: stop")
            #     if stream_resp_sql.choices[0].finish_reason == "stop":
            #         break
            #     else:
            #         pass

        return stream_resp_sql, claude_message, content_ui_message, function_ui_message, stream_resp, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency

        # except Exception as e:
        #     print(f"Error in code generation: {e}")
        #     return None, None, None, None, None, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency

    @staticmethod
    @task("code quality checking")
    async def code_quality_checking_agent(content_ui_message, function_ui_message, claude_message_new, code_check_message_history, tools, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency):
        # claude_message = {"role": "", "content": ""}
        # content_ui_message = cl.Message(content="")
        # function_ui_message = None

        # try:
        print("Starting code quality check...")
        start_time = time.time()
        response = await request_llm.send_request_streaming(code_check_message_history, tools=tools)
        end_time = time.time()
        latency += (end_time-start_time)

        print("Processing code quality check response...")
        async for stream_resp in response:
            new_delta = stream_resp.choices[0].delta
            
            print("New delta received in code check:", new_delta)  # Important for debugging
            claude_message, content_ui_message, function_ui_message, stream_resp = await UI_Utils.process_new_delta(
                new_delta, claude_message_new, content_ui_message, function_ui_message, stream_resp, True)
            
            if(stream_resp.usage.prompt_tokens!=None and stream_resp.usage.completion_tokens!=None):
                prompt_tokens = stream_resp.usage.prompt_tokens
                completion_tokens = stream_resp.usage.completion_tokens
                total_tokens = prompt_tokens + completion_tokens
                input_tokens_conv += prompt_tokens 
                output_tokens_conv += completion_tokens
                total_tokens_conv += total_tokens
                print("in code conv",total_tokens)
                print("conv input",input_tokens_conv,"conv output", output_tokens_conv,"total conv in code check",total_tokens_conv)


            # if stream_resp.choices[0].finish_reason == 'stop':
            #     print("Code quality check completed with finish_reason: stop")
            #     break

        return function_ui_message, claude_message_new, content_ui_message, function_ui_message, stream_resp, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency

        # except Exception as e:
        #     print(f"Error in code checking: {e}")
        #     return None, None, None, None, None, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency

    @staticmethod
    @task("python to sql conversion")
    async def python_to_sql_converting_agent(stream_resp_sql, function_ui_message_sql, claude_message_sql, content_ui_message_sql, current_history_sql, tools, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency):
        # claude_message = {"role": "", "content": ""}
        # content_ui_message = cl.Message(content="")
        # function_ui_message = None

        # try:
        print("Starting Python to SQL conversion...")
        start_time = time.time()
        response = await request_llm.send_request_streaming_sql_queries(current_history_sql, tools=tools)
        end_time = time.time()
        latency += (end_time-start_time)

        print("Processing SQL conversion response...")
        async for stream_resp_sql in response:
            new_delta_sql = stream_resp_sql.choices[0].delta
            print("New delta received in SQL conversion:", new_delta_sql)  # Important for debugging
            claude_message_sql, content_ui_message_sql, function_ui_message_sql,stream_resp_sql = await UI_Utils.process_new_delta(
                new_delta_sql, claude_message_sql, content_ui_message_sql, function_ui_message_sql, stream_resp_sql,False)
            
            if(stream_resp_sql.usage.prompt_tokens!=None and stream_resp_sql.usage.completion_tokens!=None):
                prompt_tokens = stream_resp_sql.usage.prompt_tokens
                completion_tokens = stream_resp_sql.usage.completion_tokens
                total_tokens = prompt_tokens + completion_tokens
                input_tokens_conv += prompt_tokens 
                output_tokens_conv += completion_tokens
                total_tokens_conv += total_tokens
                print("Token usage in SQL conversion:", total_tokens)

            # if stream_resp.choices[0].finish_reason == 'stop':
            #     print("SQL conversion completed with finish_reason: stop")
            #     break

        return claude_message_sql, content_ui_message_sql, function_ui_message_sql, stream_resp_sql, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency

        # except Exception as e:
        #     print(f"Error in SQL conversion: {e}")
        #     return None, None, None, None, email, total_tokens_conv, input_tokens_conv, output_tokens_conv, latency
