import asyncio
import time
import chainlit as cl
from gilead_loader import GileadDataLoader
from Structured_Bot.relevant_source_instructions import get_relevant_source_instructions_processor
from Structured_Bot.clickable_questions_code_all_tas import run_clickable_questions
import os
import pickle


from Structured_Bot.sending_req_to_llm import RequestLLM

sending_request_to_llm = RequestLLM()

import asyncio
import time
import chainlit as cl
import ast 
from gilead_loader import GileadDataLoader
from Structured_Bot.relevant_source_instructions import get_relevant_source_instructions_processor

BOT_TYPE_STRUCTURED = os.getenv("BOT_TYPE_STRUCTURED")
BOT_TYPE_DSG = os.getenv("BOT_TYPE_DSG")
BOTS = ast.literal_eval(os.getenv("BOTS"))

def update_files(chatbot_name):
    data = GileadDataLoader.load_multiple_folders(BOTS)
    clickable_questions = data[chatbot_name]["questions"]
    print("keys of clickable questions",clickable_questions)
    clickable_questions = clickable_questions["clickable_questions"]
    queries = clickable_questions["value"]
    clickable_questions_data_source = clickable_questions["source"].unique()
    print("data sources",clickable_questions_data_source)
    print("click keys", clickable_questions.keys())
    output_file_names = clickable_questions["output_file_names"]
    configs = data[chatbot_name]["configs"]["configs"] 
    return data,clickable_questions,clickable_questions,queries,clickable_questions_data_source,output_file_names,configs






class ClickableQuestionHandler:
    def __init__(self, handle_user_query):
        self.handle_user_query = handle_user_query
        
        self.result_data=None
    
    async def process_clickable_question(self, action: cl.Action,dataware_house, chatbot_name):
        
        data,clickable_questions,clickable_questions,queries,clickable_questions_data_source,output_file_names,configs=update_files(chatbot_name)
        print("Processing clickable question!")
        query = str(action.label)
        await cl.Message(content=query, author="You").send()
        chatbot_name = cl.user_session.get("chat_profile")
        print("chatbot_name",chatbot_name)
        if chatbot_name != BOT_TYPE_STRUCTURED and chatbot_name != BOT_TYPE_DSG:
            await self.handle_user_query(query, data_source='', gilead_prompt='')
            return
        if chatbot_name == BOT_TYPE_DSG and (query.lower() == 'weekly data health check report' or query.lower() == 'monthly data health check report'):
            query = 'Refresh ' + query
            await self.handle_user_query(query, data_source='', gilead_prompt='')
            return
        
        # First, handle sales-related questions


        sales_questions = clickable_questions[clickable_questions["source"]==clickable_questions_data_source[0]]["value"]
        print("sales_questions", sales_questions)

        if len(clickable_questions_data_source)>1:
            claims_questions = clickable_questions[clickable_questions["source"]==clickable_questions_data_source[1]]["value"]
            print("claims_questions",claims_questions)
        
        run_clickable_obj = None
        code, content_ui_message = None, None
        indication = ""
        print("query",query)
        chart_name, csv_filename = self.get_output_filenames(query, output_file_names, queries)
        print("chart name",chart_name)
        print("csv_filename",csv_filename)
        if (query.lower() in [q.lower() for q in sales_questions] or query.lower() in [q.lower() for q in claims_questions]) and "refresh" not in query.lower():
            print("inside sales")
            # Handle sales-related questions first
            run_clickable_obj = run_clickable_questions()
            sql_code,python_code, content_ui_message = await asyncio.to_thread(run_clickable_obj.run_clickable, self.result_data, query, False , chart_name, csv_filename)
            print("sql code",sql_code)
            warehouse = cl.make_async(dataware_house.get_data_from_wareshouse)
            self.result_data = await warehouse(sql_code)
            

            print("result data",self.result_data.head())
            print(f"Content UI Message: {content_ui_message}")

        # elif query.lower() in [question.lower() for question in claims_questions] and "refresh" not in query.lower():
        #     print("inside claims")
        #     # Handle claims-related questions. default indication would be the first indication in the first indication in the config
        #     run_clickable_obj = run_claims_clickable_questions()
        #     print("run_clickable_obj", run_clickable_obj)
        #     for indication_name in indications:
        #         if indication_name in query.lower():
        #             indication = indication_name
        #     else:
        #         indication = indications[0]

        #     sql_code,python_code,content_ui_message = await asyncio.to_thread(run_clickable_obj.run_claims_clickable, self.result_data, query, False, indication, chart_name, csv_filename)
        #     self.result_data=dataware_house.get_data_from_wareshouse(sql_code)
        #     print("result data",self.result_data.head())
        #     print(f"Content UI Message: {content_ui_message}")

        


        # If not a sales or claims-related query, proceed with LLM call
        if run_clickable_obj is None:
            print("Making LLM call")
            # data_source_dict = await sending_request_to_llm.ask_bedrock_claude(query)
            list_of_data_sources = configs["list_of_data_sources"]

            if len(list_of_data_sources)!=1:
                get_data_source_prompt = data[chatbot_name]["instructions"]["get_data_source"]
            else:
                get_data_source_prompt = ""
            data_source_dict = await sending_request_to_llm.get_data_source_deck(query, get_data_source_prompt)

            data_source, fields, gilead_prompt, queries_code=await get_relevant_source_instructions_processor(data_source_dict, chatbot_name)
            
            if data_source == "refresh_deck":
                return
            
            await self.handle_user_query(query, data_source, gilead_prompt)
            return
        

        

        print("before")
        print(csv_filename)
        print(chart_name)
        # Sending responses (charts & CSVs)
        async with cl.Step(name="C3PO", type="llm",default_open=True) as content_ui_message_step:
            content_ui_message_step.output = content_ui_message
            await content_ui_message_step.update()
            async with cl.Step(name="SQL Code", type="llm", show_input=False, language="python",default_open=False) as function_ui_message:
                
                # if query.lower() in sales_questions:
                #     code=sql_code
                function_ui_message.output = sql_code
                await function_ui_message.update()
                
                if run_clickable_obj:
                    if query.lower() in [question.lower() for question in sales_questions] or query.lower() in [question.lower() for question in claims_questions]:
                        await asyncio.to_thread(run_clickable_obj.run_clickable, self.result_data, query, True, chart_name , csv_filename)

        print(csv_filename)
        print(chart_name)  
        if csv_filename:
            csv_elements = [cl.File(name=csv_filename, path=f"./Structured_Bot/output_files/{csv_filename}", display="inline")]
            await cl.Message(content="File:1", elements=csv_elements).send()

        if chart_name and chart_name.endswith('.pkl'):
            # Load the Plotly figure from pickle file
            with open(f"./Structured_Bot/output_files/{chart_name}", 'rb') as f:
                fig = pickle.load(f)
            
            # Display Plotly chart in Chainlit
            chart_elements = [cl.Plotly(name="chart_" + str(time.time()), display="inline", figure=fig)]
            await cl.Message(content="Chart:1", elements=chart_elements).send()
                
        if chart_name and (chart_name.endswith('.png') or chart_name.endswith('.jpg') or chart_name.endswith('.jpeg')):
            chart_elements = [cl.Image(name="image_" + str(time.time()), display="inline", size="large", path=f"./Structured_Bot/output_files/{chart_name}")]
            await cl.Message(content="Chart:1", elements=chart_elements).send()
        else:
            print("Chart not available for this query.")
    
    def get_output_filenames(self, query, output_file_names, queries):
        print("output_file_names",output_file_names)
        print("queries",queries)
        query_map = dict(zip([q.lower() for q in queries], output_file_names))
        output_files =  eval(query_map.get(query.lower(), (None, None)))
        print(output_files)
        # print(type(output_files))
        # print(type(eval(output_files)))
        return output_files
        # return query_map.get(query.lower(), (None, None))
