from dotenv import load_dotenv
load_dotenv()
import os 
from openai import AsyncOpenAI
from traceloop.sdk.decorators import task
from traceloop.sdk import Traceloop

class LLM:
    def __init__(self):
        """
        Initializes the LLM class with API client settings.
        """
        self.client = AsyncOpenAI(
            api_key=os.getenv('DATABRICKS_TOKEN'),
            base_url=os.getenv("DATABRICKS_ENDPOINT_URL")
        )
        self.model_id =os.getenv("model_id")
    
    @task("sending request to LLM - Non streaming")
    async def send_request_non_streaming(self, system_content, user_content,email,total_tokens_conv,input_tokens_conv,output_tokens_conv):
        """
        A helper function to send a request to the LLM and return the response.
        
        Args:
            system_content (str): System prompt to guide the model.
            user_content (str): User's query.
            max_tokens (int, optional): Maximum number of tokens in the response. Defaults to 1024.
            temperature (float, optional): Sampling temperature. Defaults to 0.
            top_p (float, optional): Nucleus sampling. Defaults to 0.9.

        Returns:
            str: The response text from Claude or None if an error occurs.
        """
        print("Sending request to Claude via Databricks serving endpoint - for non streaming...")

        try:
            response = await self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": user_content}
                ],
                model=self.model_id,
                max_tokens=8192,
                temperature=0,
                top_p=1,
                stream=False  # Non-streaming request
            )

            response_text = response.choices[0].message.content  # Extract content
            print("Claude's response:", response_text)
            # Calculate token usage and pricing metrics
            prompt_tokens = response.usage.prompt_tokens
            completion_tokens = response.usage.completion_tokens
            total_tokens = prompt_tokens + completion_tokens
            input_tokens_conv+=prompt_tokens 
            output_tokens_conv+=completion_tokens
            total_tokens_conv+=total_tokens
            print("in non streaming call total tokens",total_tokens)
            prompt_cost = prompt_tokens * 0.000003
            completion_cost = completion_tokens * 0.000015
            total_cost = prompt_cost + completion_cost
            association_properties = {
            "Total_Input_Tokens": input_tokens_conv,  # Example metric
            "Total_Output_Tokens": output_tokens_conv,  # Example metric
            "Total_Tokens": total_tokens_conv,
            "Tokens_per_conversation": round(total_tokens_conv / 6, 2),
            "Input_Token_Usage_Cost": f"${prompt_cost}",  # Adding dollar symbol
            "Output_Token_Usage_Cost": f"${completion_cost}",  # Adding dollar symbol
            "Total_Token_Usage_Cost": f"${total_cost}"
        }
            Traceloop.set_association_properties(
                association_properties
                )
            return response_text,email,total_tokens_conv,input_tokens_conv,output_tokens_conv

        except Exception as e:
            print(f"ERROR: Can't invoke '{self.model_id}'. Reason: {e}")
            return None

    @task("sending request to LLM - streaming")
    async def send_request_streaming(self, current_history, tools=[]):
        """
        A helper function to send a request to the LLM and return the response.
        
        Args:
            system_content (str): System prompt to guide the model.
            user_content (str): User's query.
            max_tokens (int, optional): Maximum number of tokens in the response. Defaults to 1024.
            temperature (float, optional): Sampling temperature. Defaults to 0.
            top_p (float, optional): Nucleus sampling. Defaults to 0.9.

        Returns:
            str: The response text from Claude or None if an error occurs.
        """
        print("Sending request to Claude via Databricks serving endpoint... - for streaming")

        try:
            response = await self.client.chat.completions.create(
                messages=current_history,
                model=self.model_id,
                max_tokens=8192,
                temperature=0,
                top_p=1,
                tools=tools,
                stream=True  # Non-streaming request
            )

            # response_text = response.choices[0].message.content  # Extract content
            # print("Claude's response:", response_text)
            return response

        except Exception as e:
            print(f"ERROR: Can't invoke in streaming response request '{self.model_id}'. Reason: {e}")
            return None
    

    async def send_request_streaming_sql_queries(self, current_history, tools=[]):
        """
        A helper function to send a request to the LLM and return the response.
        
        Args:
            system_content (str): System prompt to guide the model.
            user_content (str): User's query.
            max_tokens (int, optional): Maximum number of tokens in the response. Defaults to 1024.
            temperature (float, optional): Sampling temperature. Defaults to 0.
            top_p (float, optional): Nucleus sampling. Defaults to 0.9.

        Returns:
            str: The response text from Claude or None if an error occurs.
        """
        print("Sending request to Claude via Databricks serving endpoint... - for streaming")

        try:
            response = await self.client.chat.completions.create(
                messages=current_history,
                model=self.model_id,
                max_tokens=8192,
                temperature=0,
                top_p=1,
                tools=tools,
                stream=True  # Non-streaming request
            )

            # response_text = response.choices[0].message.content  # Extract content
            # print("Claude's response:", response_text)
            return response

        except Exception as e:
            print(f"ERROR: Can't invoke in streaming response request '{self.model_id}'. Reason: {e}")
            return None


    

# class NonStreamingLLM(LLM):

    
#     async def get_data_source(self, user_question_content, system_content):
#         """
#         Calls the LLM to get a response to a user's question.

#         Args:
#             user_question_content (str): The user's question.
#             system_content (str): The system prompt.

#         Returns:
#             str: The model's response text.
#         """
#         return await self._send_request(system_content, user_question_content)

#     async def get_relevant_business_rules(self, all_business_rules, user_query_relevant_rules, agent_1_instructions):
#         """
#         Calls the LLM to fetch relevant business rules based on a user's query.

#         Args:
#             all_business_rules (str): The set of all business rules.
#             user_query_relevant_rules (str): The user's query.
#             agent_1_instructions (str): The instruction template for extracting rules.

#         Returns:
#             str: Relevant business rules.
#         """
#         formatted_prompt = agent_1_instructions.format(all_business_rules=all_business_rules)
#         return await self._send_request(formatted_prompt, user_query_relevant_rules, max_tokens=3000)
    

# class StreamingLLM(LLM):

#     async def insights_module(user_message_copy, saved_files_lst):
#     # Initialize messages and client
#         claude_message = {"role": "", "content": ""}
#         content_ui_message = cl.Message(content="")
#         function_ui_message = None

#         # Process files
#         data_list = [] 
#         for file in saved_files_lst:
#             try:
#                 if file.endswith('.csv'):
#                     df = pd.read_csv(file)
#                 elif file.endswith('.xlsx') or file.endswith('.xls'):
#                     df = pd.read_excel(file)
#                 else:
#                     continue
                
#                 data_list.append({
#                     "File Name": file,
#                     "File's Content": df.to_dict(orient='records')
#                 })
#             except Exception as e:
#                 print(f"Error processing file {file}: {e}")

#         # Prepare JSON data
#         json_data_combined = json.dumps(data_list, indent=4)
#         parsed_data = json.loads(json_data_combined)
#         json_data_combined = [file_data["File's Content"] for file_data in parsed_data]

#         # Debugging prints
#         print("Combined Data in JSON Format:")
#         print(json.dumps(json_data_combined, indent=4))
#         print(f'User Query: {user_message_copy}')
#         print(f'Output Files: {saved_files_lst}')
#         print(f'Json Data Length: {len(json_data_combined)}')


#         # Prepare system and user messages
#         insights_header = "Key Insights: " + '\n' * 5
#         system_prompt = insights_module_system_prompt.format(insights_header=insights_header)

#         # Prepare conversation history
#         current_history = [
#             {
#                 "role": "system",
#                 "content": system_prompt
#             },
#             {
#                 "role": "user",
#                 "content": f"User question: {user_message_copy}\nData: {str(json_data_combined)}"
#             }
#         ]

#         # Prepare tool functions (if any)
#         # active_bot_functions = []  # Add any specific tools if needed

#         try:
#             # Stream the response
#             response = self._send_request(current_history, tools=active_bot_functions)

#             # Process streaming response
#             async for stream_resp in response:
#                 # Check if there's a delta to process
#                 if stream_resp.choices[0].delta:
#                     new_delta = stream_resp.choices[0].delta
#                     claude_message, content_ui_message, function_ui_message = await process_new_delta(
#                         new_delta, claude_message, content_ui_message, function_ui_message
#                     )

#                 # Optional: Check for stream completion
#                 if stream_resp.choices[0].finish_reason == 'stop':
#                     break

#             # Return the final insights
#             return claude_message, content_ui_message

#         except Exception as e:
#             print(f"Error in insights generation: {e}")
#             error_message = cl.Message(content=f"An error occurred while generating insights: {str(e)}")
#             return {"role": "assistant", "content": str(e)}, error_message
        