import asyncio
import os
import chainlit as cl
import time
from datetime import datetime, timedelta
import ast

from Structured_Bot.slides_refresh_code_all_tas import Update_Onc_Sales_Deck
from Structured_Bot.slides_refresh_code_all_tas import Update_Onc_Claims_Deck
from ppt_generator import GeneratePPT
from gilead_loader import GileadDataLoader


BOTS = ast.literal_eval(os.getenv("BOTS"))
data = GileadDataLoader.load_multiple_folders(BOTS)
print("gilead loader keys in agent tools ", data.keys())
chatbot_name = os.getenv("BOT_TYPE_STRUCTURED", "Structured")
configs = data[chatbot_name]["configs"]
configs = configs.get("configs", "")
deck_creation_classes = configs["deck_creation_classes"]


class DeckCreator:
    def __init__(self):
        """Load sales and claims data when the class is instantiated"""
        # gilead_data = GileadDataLoader.load_sales_and_claims_data()

        # self.tro_sales_data = gilead_data["tro_sales_data"]  
        # self.ddd_sales_data = gilead_data["ddd_sales_data"] 
        # self.tnbc_tps_data = gilead_data["tnbc_tps_data"]
        # self.perst_data_old_batch = gilead_data["perst_data_old_batch"]

    async def DeckSource_Sales(self):
        """Creates the 867 Sales Deck."""
        print("Inside create 867 sales deck function!!")

        if "Update_Onc_Sales_Deck" in deck_creation_classes:
            # await cl.message(content="Refreshing Sales Deck please wait ...").send()
            return await self._process_deck_creation(Update_Onc_Sales_Deck(),"867_sales_refreshed_ppt.pptx")
        else:
            return None

    async def DeckSource_Claims(self):
        """Creates the Claims Deck."""
        print("Inside create claims deck function!!")
        if "Update_Onc_Claims_Deck" in deck_creation_classes:
            # await cl.message(content="Refreshing Claims Deck please wait ...").send()
            return await self._process_deck_creation(Update_Onc_Claims_Deck(), "claims_refreshed_ppt.pptx")
        else:
            return None
    async def DeckSource_Create(self):
        """Create the Deck for open ended questions"""
        print("Inside Deck Creation for user question function!!")
        if "GeneratePPT" in deck_creation_classes:
            # filename=cl.user_session.get("deck_params").get("filename")
            filename=cl.user_session.get("deck_filename")
            return await self._process_deck_creation(GeneratePPT(),filename)
        else:
            return None



    async def _process_deck_creation(self, update_deck_obj, filename):
        """
        Handles the common logic for updating the deck, sending the file, and deleting it.

        Parameters:
        - update_deck_obj: The update deck class instance (UpdateSalesDeck or UpdateClaimsDeck).
        - update_args: List of arguments required for updating the deck.
        - filename: The name of the generated file.

        Returns:
        - True if deck creation is successful, False otherwise.
        """
        try:
            # Unpack arguments correctly
            await asyncio.to_thread(update_deck_obj.update_deck)

            # Check if the file exists and process it
            if os.path.exists(filename):
                print(f"File {filename} exists, preparing to send.")
                await asyncio.sleep(1)

                elements = [cl.File(name=filename, path=f"./{filename}", display="inline")]
                await cl.Message(content=f" File: {filename}", elements=elements).send()

                await asyncio.sleep(1)
                await self._delete_file(filename)

                return True

            print(f"File {filename} not found after processing.")
            return False
        except TypeError as e:
            print(f"Error: {e} - Ensure correct arguments are passed to update_deck.")

    async def _delete_file(self, filename):
        """Deletes the given file if it exists."""
        try:
            if os.path.exists(filename):
                os.remove(filename)
                print(f"{filename} has been deleted.")
            else:
                print(f"{filename} does not exist.")
        except Exception as e:
            print(f"Error while deleting {filename}: {str(e)}")



from datetime import datetime, timedelta

class SalesPerformanceMetrics:
    @staticmethod
    def get_most_recent_friday(input_date):
        """Returns the most recent Friday on or before the given date."""
        target_date = datetime.strptime(input_date, '%Y-%m-%d')
        return target_date - timedelta(days=(target_date.weekday() - 4) % 7)

    @staticmethod
    def calculate_rolling_previous_dates(input_date, rolling_weeks):
        """
        General function to calculate rolling and previous period date ranges.
        
        Args:
            input_date (str): Input date in 'YYYY-MM-DD' format.
            rolling_weeks (int): Number of weeks for the rolling period.

        Returns:
            dict: Dictionary containing start and end dates for rolling and previous periods.
        """
        most_recent_friday = SalesPerformanceMetrics.get_most_recent_friday(input_date)
        
        rolling_start = most_recent_friday - timedelta(weeks=rolling_weeks - 1)
        rolling_end = most_recent_friday
        
        previous_end = rolling_start - timedelta(days=7)
        previous_start = previous_end - timedelta(weeks=rolling_weeks - 1)
        
        return {
            f'start_r{rolling_weeks}w': rolling_start.strftime('%Y-%m-%d'),
            f'end_r{rolling_weeks}w': rolling_end.strftime('%Y-%m-%d'),
            f'start_p{rolling_weeks}w': previous_start.strftime('%Y-%m-%d'),
            f'end_p{rolling_weeks}w': previous_end.strftime('%Y-%m-%d')
        }


    @staticmethod
    def calculate_r12m_p12m(input_date):
        """Calculates R12M (Rolling 12 Months) and P12M (Previous 12 Months) date ranges."""
        most_recent_friday = SalesPerformanceMetrics.get_most_recent_friday(input_date)
        r12m_start = most_recent_friday - timedelta(weeks=51)
        p12m_end = r12m_start - timedelta(days=7)
        p12m_start = p12m_end - timedelta(weeks=51)
        
        print("At R12m")
        return {
            'r12m_start_date': r12m_start.strftime('%Y-%m-%d'),
            'r12m_end_date': most_recent_friday.strftime('%Y-%m-%d'),
            'p12m_start_date': p12m_start.strftime('%Y-%m-%d'),
            'p12m_end_date': p12m_end.strftime('%Y-%m-%d')
        }

    @staticmethod
    def get_qtd_dates(input_date):
        """Calculates Quarter-To-Date (QTD) date ranges."""
        given_date = datetime.strptime(input_date, '%Y-%m-%d')
        quarter_starts = {1: 'Q1', 4: 'Q2', 7: 'Q3', 10: 'Q4'}
        
        qtd_dates = {}
        current_year = given_date.year
        end_date = given_date.strftime('%Y-%m-%d')
        
        for month, quarter in quarter_starts.items():
            quarter_start = datetime(current_year, month, 1)
            prev_quarter_start = datetime(current_year - 1, month, 1)
            start_date = quarter_start if given_date >= quarter_start else prev_quarter_start
            
            qtd_dates[f'start_{quarter.lower()}'] = start_date.strftime('%Y-%m-%d')
            qtd_dates[f'end_{quarter.lower()}'] = end_date
        
        return qtd_dates

        
    @staticmethod
    def calculate_date_ranges(input_date):
        """
        Computes date ranges for different periods:
        - Rolling 12 months (R12M) and previous 12 months (P12M)
        - Rolling 4 weeks (R4W) and previous 4 weeks (P4W)
        - Rolling 13 weeks (R13W) and previous 13 weeks (P13W)
        - Quarter-to-date (QTD)
        """
        return {
            **SalesPerformanceMetrics.calculate_r12m_p12m(input_date),
            **SalesPerformanceMetrics.calculate_rolling_previous_dates(input_date, 4),
            **SalesPerformanceMetrics.calculate_rolling_previous_dates(input_date, 13),
            **SalesPerformanceMetrics.get_qtd_dates(input_date),
        }