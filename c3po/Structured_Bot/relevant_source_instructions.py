import chainlit as cl
import os
import ast
from gilead_loader import GileadDataLoader
BOTS = ast.literal_eval(os.getenv("BOTS"))
data = GileadDataLoader.load_multiple_folders(BOTS)
print("DEBUG data loaded for multiple folders in relevent source instructions:", data.keys())
list_of_data_sources = data[BOTS[0]]["configs"]["configs"]["list_of_data_sources"]
result = any("DeckSource" in Source for Source in list_of_data_sources)
if result:
    from Structured_Bot.agent_tools import DeckCreator
    deck_creator = DeckCreator()

class DataSourceProcessor:
    """
    Base class to process different data sources and execute specific tasks
    """
    def __init__(self, data_source_dict,chatbot_name):
        if isinstance(data_source_dict, str):
            # Only use eval() if data_source_dict is a string (rare case)
            self.data_source_dict = eval(data_source_dict)
        else:
            # If it's already a dictionary, use it directly
            self.data_source_dict = data_source_dict

        self.data_source = self.data_source_dict.get('source', '')
        self.fields = self.data_source_dict.get('fields', [])

        self.instructions = data[chatbot_name]["instructions"]
        self.descriptions = data[chatbot_name]["tools"]
        for item in self.descriptions:
            print(item)
        self.configs = data[chatbot_name]["configs"]
        print("configs",self.configs)
        self.list_of_data_sources = self.configs["configs"]["list_of_data_sources"]
        # self.business_rules = instructions["sales_instructions"]
        # self.initial_prompt_gilead = instructions["initial_prompt_gilead"]
        # self.claims_instructions = instructions["claims_instructions"]
        # self.sales_column_descriptions = descriptions["sales_column_descriptions"]
        # self.claims_column_descriptions = descriptions["claims_column_descriptions"]
        # self.sales_queries_code=descriptions["sales_queries_code"]
        # self.claims_queries_code=descriptions["claims_queries_code"]
    
    async def process(self):
        raise NotImplementedError("Subclasses should implement this method")



class DataSourceObject(DataSourceProcessor):
    """
    Class to process data sources
    """

    async def process(self,chatbot_name):

        try:

            data = GileadDataLoader.load_multiple_folders([chatbot_name])

            self.instructions = data[chatbot_name]["instructions"]
            self.descriptions = data[chatbot_name]["tools"]
            for item in self.descriptions:
                print(item)
            self.configs = data[chatbot_name]["configs"]
            print("configs",self.configs)
            self.list_of_data_sources = self.configs["configs"]["list_of_data_sources"]

            if  isinstance(self.data_source, str) and "DataSource" in self.data_source and self.data_source in self.list_of_data_sources :
                business_rules = self.data_source + "_Instructions"
                column_descriptions = self.data_source + "_Column_Descriptions"

                gilead_prompt = f"{self.instructions[business_rules]}\n\n{self.descriptions[column_descriptions]}"
                queries_code = self.descriptions[self.data_source + "_Queries_Code"]
                print("gilead_prompt :", gilead_prompt)
                print("queries_code :",queries_code)
                return self.data_source, self.fields, gilead_prompt, queries_code
            
            elif isinstance(self.data_source, list):
                if len(self.data_source) == 1 and self.data_source[0] in self.list_of_data_sources:
                    # Handle as a single data source (same as string case)
                    business_rules = self.data_source[0] + "_Instructions"
                    column_descriptions = self.data_source[0] + "_Column_Descriptions"
                    gilead_prompt = f"{self.instructions[business_rules]}\n\n{self.descriptions[column_descriptions]}"
                    queries_code = self.descriptions[self.data_source[0] + "_Queries_Code"]
                    print("gilead_prompt :", gilead_prompt)
                    print("queries_code :", queries_code)
                    return self.data_source[0], self.fields, gilead_prompt, queries_code
                elif len(self.data_source) == 2 and all(item in self.list_of_data_sources[:2] for item in self.data_source):
                    data_source_1 = self.data_source[0]
                    data_source_2 = self.data_source[1]

                    business_rules_1 = data_source_1 + "_Instructions"
                    column_descriptions_1 = data_source_1 + "_Column_Descriptions"

                    business_rules_2 = data_source_2 + "_Instructions"
                    column_descriptions_2 = data_source_2 + "_Column_Descriptions"

                    gilead_prompt = f"Business Rules of {data_source_1} : {self.instructions[business_rules_1]}\n\n{self.descriptions[column_descriptions_1]}\n\n Business Rules of {data_source_2} : {self.instructions[business_rules_2]}\n\n{self.descriptions[column_descriptions_2]}"
                    queries_code = self.descriptions[data_source_1 + "_Queries_Code"]
                    print("gilead_prompt :", gilead_prompt)
                    print("queries_code :",queries_code)
                    return self.data_source, self.fields, gilead_prompt, queries_code
                elif len(self.data_source) > 2 and all(item in self.list_of_data_sources for item in self.data_source):
                    # Handle 3 or more data sources
                    gilead_prompt = ""
                    queries_code = {}
                    for ds in self.data_source:
                        business_rules = ds + "_Instructions"
                        column_descriptions = ds + "_Column_Descriptions"
                        gilead_prompt += f"Business Rules of {ds}: {self.instructions.get(business_rules, '')}\n\n{self.descriptions.get(column_descriptions, '')}\n\n"
                        queries_code[ds] = self.descriptions.get(ds + "_Queries_Code", "")
                    print("gilead_prompt :", gilead_prompt)
                    print("queries_code :", queries_code)
                    return self.data_source, self.fields, gilead_prompt, queries_code
            

            
            elif isinstance(self.data_source, str) and "DeckSource" in self.data_source and self.data_source in self.list_of_data_sources:
                    cl.user_session.set("is_executing", True)
                    loader_msg =cl.Message(
                        content="Executing function...",
                        elements=[cl.CustomElement(name="FunctionExecutionLoader", props={
                            "isExecuting": cl.user_session.get("is_executing"),
                            "functionName": "Generating Deck",
                            "message": "Please wait..."
                        })]
                    )
                    await loader_msg.send()

                    method_call = f"deck_creator.{self.data_source}()"
                    print("calling deck creation")
                    gilead_prompt = await eval(method_call)
                    cl.user_session.set("is_executing", False)
                    await loader_msg.remove() 
                     # Send the final confirmation message
                    await cl.Message(
                        content="The deck has been refreshed successfully!",
                        elements=[cl.CustomElement(name="FunctionExecutionLoader", props={
                            "isExecuting": False,
                            "functionName": "Deck Generation",
                            "message": "Please wait..."
                        })]).send()

                    # await loader_msg.send()
                    print("done with deck creation")
                    return "refresh_deck", [], gilead_prompt , ""

            else:
                gilead_prompt = self.instructions["DataSource_Sales_Instructions_Combined"]
                queries_code = ""
                return self.data_source, self.fields, gilead_prompt, queries_code
            
        except Exception as e:
            print("Exception - ", e)
            raise ValueError(f"Unsupported data source type: {self.data_source}")
        

        


async def get_relevant_source_instructions_processor(data_source_dict, chatbot_name):
    """
    Processes the input data source dictionary, evaluates the fields, and generates
    a tailored prompt or executes specific tasks (like refreshing decks).
    """
    print("here in get data source!!!!!", data_source_dict)

    # Ensure it's a dictionary
    if isinstance(data_source_dict, str):
        data_source_dict = eval(data_source_dict)  # Convert to dictionary only if it's a string
    print("here is the data source dict",data_source_dict)
    data_source = data_source_dict['source']
    
    # Flatten if it is a single-element list
    if isinstance(data_source, list) and len(data_source) == 1:
        data_source = data_source[0]
    return await DataSourceObject(data_source_dict, chatbot_name).process(chatbot_name)
