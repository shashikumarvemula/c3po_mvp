from Semi_Structured_Bot.HCP_Loader import HCPFilesLoader
from Semi_Structured_Bot.opensearch_execution import OpensearchExecutionManager
opensearch = OpensearchExecutionManager()
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
import chainlit as cl

class BYODProcessor:
    """
    This class processes documents for the BYOD (Bring Your Own Device) chatbot.
    It extracts relevant information from the documents and prepares it for further processing.
    """

    def __init__(self):
        print("BYODProcessor initialized")
        # self.document = None

    async def divide_text_into_chunks(self,file_content):
        # print("file content in divide text into chunks",file_content)
        splitter = RecursiveCharacterTextSplitter(chunk_size=8000, chunk_overlap=200)
        splits = splitter.split_text(file_content) 
        print("in divide text in to chunks",len(splits)) # Use split_text instead of split_documents for plain strings
        return splits

    async def process_document_and_ingest(self,file_content, user_id, thread_id, index_name):

        print(" in Processing document ")
        # print(file_content)
        cl.user_session.set("sleep_time", 3)

        chunks = await self.divide_text_into_chunks(file_content)
        print("chunks : ",chunks[0])
        print(len(chunks[0]))

        embeddings = opensearch.get_batch_embeddings(chunks)
        print("batch embeddings generated : ",len(embeddings[0]),len(embeddings))
        print("batch embeddings : ",embeddings)
        print("first element in embeddings : ",embeddings[0])
        print(len(embeddings))

        if len(chunks)  <= 50 :
            cl.user_session.set("sleep_time", 5)
        elif len(chunks) > 50 and len(chunks) <= 100:
            cl.user_session.set("sleep_time", 10)
        else:
            cl.user_session.set("sleep_time", 15)

        

        try:
            print("Starting data ingestion...")
            user_thread = user_id + "_" + thread_id
            print("user_thread : ",user_thread)
            success, errors = await opensearch.ingest_data_to_opensearch(user_thread, chunks, embeddings, index_name)
            print(f"Data ingestion complete.")
            print(f"Successfully indexed: {success} documents")
            print(f"Failed to index: {errors} documents")
            return True
        except Exception as e:
            print(f"Error during data ingestion: {e}")

        return False
    
    
