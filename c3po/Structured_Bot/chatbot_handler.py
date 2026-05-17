import pandas as pd
import chainlit as cl
from chainlit.input_widget import Select
from gilead_loader import GileadDataLoader
from Structured_Bot.S3_code import LoadingFilesFromS3
import os
from CopyFolder import copy_folders
import ast


BOTS = ast.literal_eval(os.getenv("BOTS"))
data = GileadDataLoader.load_multiple_folders(BOTS)


# import inspect
# methods = inspect.getmembers(gilead_loader, predicate=inspect.ismethod)
# for name, _ in methods:
#     print("methods",name)  


BOT_TYPE_STRUCTURED = "Structured"
BOT_TYPE_SEMI_STRUCTURED = "Semi_Structured"
BOT_TYPE_BYOD = "BYOD"
BOT_TYPE_PMR="PMR"
BOT_TYPE_EARLY_EXP=os.getenv("BOT_TYPE_EARLY_EXP")  #"Early_Exp"
BOT_TYPE_RM_INSIGHTS="RM_insights"
BOT_TYPE_PHYSICIAN_OPINIONS_V2="Physician_opinions_v2"
BOT_TYPE_DSG=os.getenv("BOT_TYPE_DSG")  #"DSG"
BOT_TYPE_MARKET_MAP="PMR_MARKET_MAP"
BOT_TYPE_PBC_MARKET_RESEARCH=os.getenv("BOT_TYPE_PBC_MARKET_RESEARCH")  #"PBC_Market_Research"
#import from the file when we initialized

class ChatbotHandler:
    def __init__(self):
        """
        Initializes the ChatbotHandler with necessary configurations.
        """
        self.gilead_loader = GileadDataLoader.load_multiple_folders(BOTS)
        self.clickable_questions = self.gilead_loader[BOT_TYPE_STRUCTURED]["questions"]["clickable_questions"]
        self.clickable_questions_data_source = self.clickable_questions["source"].unique()
        self.configs = self.gilead_loader[BOT_TYPE_STRUCTURED]["configs"]["configs"]
        print("Configs loaded:", self.configs)

        # Initialize S3 loader

    async def send_questions(self, category_list, clickable_questions):
        """
        Sends categorized clickable questions from a preloaded DataFrame.
        :param category_list: List of categories to display questions for.

        """
        questions_data = clickable_questions  # Dynamically load questions

        print("DEBUG: Type of questions_data ->", type(questions_data))  # Debugging
        print("DEBUG: Content of questions_data ->", questions_data)  # Debugging

        if isinstance(questions_data, dict):
            if "clickable_questions" in questions_data:
                questions_df = pd.DataFrame(questions_data["clickable_questions"])
            elif "Clickable_questions" in questions_data:
                questions_df = pd.DataFrame(questions_data["Clickable_questions"])
            else:
                raise KeyError("Missing key 'clickable_questions' in questions_data dictionary.")
        elif isinstance(questions_data, pd.DataFrame):
            questions_df = questions_data
        else:
            raise TypeError(f"Unexpected type for questions_data: {type(questions_data)}")

        # Drop NaN values in 'value' and 'label' columns
        questions_df = questions_df.dropna(subset=['value', 'label'])

        for category in category_list:
            category_questions = questions_df[questions_df['content'] == category]
            print("\n"*5)
            print(f"{category} and {category_questions}")
            
            actions = [
                cl.Action(name="Clickable_Question", value=row['value'], label=row['label'],payload=row.get('payload', {}),icon="mouse")
                for _, row in category_questions.iterrows()
            ]
            message = cl.Message(content=f"**{category}**", actions=actions)
            await message.send()

    async def gilead_bot(self):
        """
        Handles Gilead bot logic, displaying database updates and categorized questions.
        """
        # message_content = f"""
        # **C3PO Database is updated as of {formatted_latest_weekend_date}, and contains:**
        # **867 Sales data:** Updated through the week ending - {formatted_last_867_data}
        # **DDD data:** Updated through the week ending - {formatted_latest_week_end_date_enh}
        # **Claims data (TNBC/HR+/HER2-):** Updated through November 2024 \n\n\n You can download the latest report by asking C3PO to "Refresh the Sales deck".\n\n\n Choose from the following questions or type your own.
        # """

        # message = cl.Message(content=message_content)
        # await message.send()

        
        clickable_questions = data[BOT_TYPE_STRUCTURED]["questions"]
        clickable_questions_content = clickable_questions["clickable_questions"]["content"].dropna().unique()
        print("\n"*10)
        print("At onc structured function: ", clickable_questions_content)
        print("\n"*10)
        await self.send_questions(
            clickable_questions_content,
            clickable_questions  # Pass the correct loader
        )

    async def HCP_Opinions_bot(self):
        """
        Handles HCP Opinions bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""


            HCP - health care providers 
            contains the latest data on HCP opinions and insights.
            

         """

        message = cl.Message(content=message_content)##disable_feedback=True
        await message.send()

    async def DSG_bot(self):
        """
        Handles HCP Opinions bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""

            DSG - Data Strategy and Governance.

         """
        message = cl.Message(content=message_content)##disable_feedback=True
        await message.send()
        clickable_questions = data[BOT_TYPE_DSG]["questions"]
        print("clickable question for PMR: ", clickable_questions)
        clickable_questions_content = clickable_questions["clickable_questions"]["content"].dropna().unique()
        print("\n"*10)
        print("At DS&G onc structured function: ", clickable_questions_content)
        print("\n"*10)
        await self.send_questions(
            clickable_questions_content,
            clickable_questions  # Pass the correct loader
        )

        

    async def BYOD_bot(self):
        """
        Handles bring your own data .
        """
        message_content = f"""
            BYOD - BRING YOUR OWN DATA
            USE the BYOD bot to answer questions related to your own data.
            this is working now only for the unstructed questions.
            Please upload your data in the following format:
            1. DOC
            2. DOCX
            3. JSON 
            4. PDF
            5. PPT
            6. PPTX
            7. TXT
         """
        message = cl.Message(content=message_content)##disable_feedback=True
        await message.send()

    async def PMR_bot(self):
        """
        Handles PMR bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""
            PMR - Primary Market Research 
            contains the data on Primary Market Research.
         """

        # message = cl.Message(content=message_content)
        # await message.send()
        clickable_questions = data[BOT_TYPE_PMR]["questions"]
        print("clickable question for PMR: ", clickable_questions)
        clickable_questions_content = clickable_questions["clickable_questions"]["content"].dropna().unique()
        print("\n"*10)
        print("At onc structured function: ", clickable_questions_content)
        print("\n"*10)
        await self.send_questions(
            clickable_questions_content,
            clickable_questions  # Pass the correct loader
        )
    
    async def early_exp_bot(self):
        """
        Handles early exp bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""
            Datroway and Enhertu Early Experience Study
         """

        message = cl.Message(content=message_content)
        await message.send()

        clickable_questions = data[BOT_TYPE_EARLY_EXP]["questions"]
        print("clickable question for early experience: ", clickable_questions)
        clickable_questions_content = clickable_questions["clickable_questions"]["content"].dropna().unique()
        print("\n"*10)
        print("At onc structured function: ", clickable_questions_content)
        print("\n"*10)
        await self.send_questions(
            clickable_questions_content,
            clickable_questions  # Pass the correct loader
        )
    async def PBC_market_research_bot(self):
        """
        Handles PBC market research bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""
            PBC Market Research - answers questions on PBC market research transcripts.
         """
        
        message = cl.Message(content=message_content)
        await message.send()

    async def RM_insights_bot(self):
        """
        Handles RM insights bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""
            RM insights - RM insights 
            contains the data on RM insights.
         """
    async def Physician_Opinions_V2_bot(self):
        """
        Handles Physician_Opinions_V2 bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""
            Physician_Opinions_V2 - Physician Opinions 
            contains the data on Physician Opinions.
         """

        message = cl.Message(content=message_content)
        await message.send()
    
    async def market_map_bot(self):
        """
        Handles market map bot logic, displaying database updates and categorized questions.
        """
        message_content = f"""
            MARKET MAP - Market Map 
         """

        message = cl.Message(content=message_content)
        await message.send()


    async def start_chat(self):
        """
        Initializes chat session, associates users with the appropriate chatbot, and displays questions.
        """
        email = cl.user_session.get("user").identifier

        chat_profile = cl.user_session.get("chat_profile")
    
        if chat_profile == BOT_TYPE_STRUCTURED:
            print("Structured bot is called")
            cl.user_session.set("chatbot", "Structured")
            await self.gilead_bot()   

        elif chat_profile == BOT_TYPE_DSG:
            print("DSG Structured bot is called")
            cl.user_session.set("chatbot", "DS&G")
            await self.DSG_bot()    

        elif chat_profile == BOT_TYPE_SEMI_STRUCTURED:
            print("Semi Structured bot is called")
            cl.user_session.set("chatbot", "Semi_Structured")
            await self.HCP_Opinions_bot()

        elif chat_profile == BOT_TYPE_BYOD:
            cl.user_session.set("chatbot", "BYOD")
            print("BYOD bot is called")
            await self.BYOD_bot()
        
        elif chat_profile == BOT_TYPE_PMR:
            print("PMR bot is called")
            cl.user_session.set("chatbot", "PMR")
            await self.PMR_bot()

        elif chat_profile==BOT_TYPE_MARKET_MAP:
            print("PMR MARKET MAP bot is called")
            cl.user_session.set("chatbot", "PMR_MARKET_MAP")
            await self.market_map_bot()
        
        elif chat_profile==BOT_TYPE_EARLY_EXP:
            print("Early Experience bot is called")
            cl.user_session.set("chatbot", "Early_Exp")
            await self.early_exp_bot()
        
        elif chat_profile==BOT_TYPE_PBC_MARKET_RESEARCH:
            print("PBC Market Research bot is called")
            cl.user_session.set("chatbot", "PBC_MARKET_RESEARCH")
            await self.PBC_market_research_bot()



        cl.user_session.set("message_history", [{"role": "system", "content": "YOU ARE AN ANALYST AGENT, YOU ARE HERE TO HELP THE USER TO UNDERSTAND THE DATA AND ANSWER HIS QUESTIONS IF YOU DONT HAVE ANY CONTEXT YOU SHOULD REPLY WITH PLESAE GIVE CONTEXT. if the user question has some intent regarding the issue reply politely the issue as our team will fix this issue soon PLEASE ESCALATE THE ISSUE."},{"role": "user", "content": ""}])

        await cl.Message(content="Trust but verify each step of C3PO to ensure that your question is interpreted and analyzed correctly.").send()



    async def setup_agent(self, settings):
        """
        Updates chatbot based on user settings.
        """
        chatbot_name = cl.user_session.get("chatbot", "Structured")
        cl.user_session.set("chatbot", chatbot_name)

        cl.user_session.set("message_history", [{"role": "system", "content": "YOU ARE AN ANALYST AGENT, YOU ARE HERE TO HELP THE USER TO UNDERSTAND THE DATA AND ANSWER HIS QUESTIONS IF YOU DONT HAVE ANY CONTEXT YOU SHOULD REPLY WITH PLESAE GIVE CONTEXT. if the user question has some intent regarding the issue reply politely the issue as our team will fix this issue soon PLEASE ESCALATE THE ISSUE."},{"role": "user", "content": ""}])
        await cl.Message(content="").send()

        if chatbot_name == BOT_TYPE_STRUCTURED:
            await self.gilead_bot()

        elif chatbot_name==BOT_TYPE_DSG:
            await self.DSG_bot()
        
        elif chatbot_name==BOT_TYPE_PMR:
            await self.PMR_bot()

        elif chatbot_name == BOT_TYPE_SEMI_STRUCTURED:
            await self.HCP_Opinions_bot()

        elif chatbot_name == BOT_TYPE_BYOD:
            await self.BYOD_bot()
        
        elif chatbot_name==BOT_TYPE_PMR:
            await self.PMR_bot()
        
        elif chatbot_name==BOT_TYPE_EARLY_EXP:
            await self.early_exp_bot()
        
        elif chatbot_name==BOT_TYPE_PBC_MARKET_RESEARCH:
            await self.PBC_market_research_bot()

        elif chatbot_name==BOT_TYPE_MARKET_MAP:
            await self.market_map_bot()

        elif chatbot_name==BOT_TYPE_RM_INSIGHTS:
            await self.RM_insights_bot()
        elif chatbot_name==BOT_TYPE_PHYSICIAN_OPINIONS_V2:
            await self.Physician_Opinions_V2_bot()
            
        


