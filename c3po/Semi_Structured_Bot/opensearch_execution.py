import asyncio
import time
from datetime import datetime
import chainlit as cl
from Semi_Structured_Bot.HCP_Loader import HCPFilesLoader
import pandas as pd
import requests
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import json
import os
import openai
import logging
from dotenv import load_dotenv
import pandas as pd
import concurrent.futures
import awswrangler as wr
import boto3
import numpy as np
import copy
boto3.setup_default_session(region_name="us-west-2")

hcp_loader = HCPFilesLoader()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)


def get_secret(secret_name, **kwargs):

        """
        Retrieve individual secrets from AWS Secrets Manager using the get_secret_value API.
        This function assumes the stack mentioned in the source code has been successfully deployed.
        This stack includes 7 secrets, all of which have names beginning with "mySecret".

        :param secret_name: The name of the secret fetched.
        :type secret_name: str
        """
        session = boto3.Session()
        client = session.client("secretsmanager", region_name="us-west-2")
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            if len(kwargs.keys()) > 0:
                key = kwargs['key']
                print(key)
                secret = json.loads(str(get_secret_value_response["SecretString"]))[key]
            else:
                 secret = get_secret_value_response["SecretString"]
            return secret

        except client.exceptions.ResourceNotFoundException:
            msg = f"The requested secret {secret_name} was not found."
            return msg
        except Exception as e:
            raise e

secret_name = os.getenv("secret_name")
print("secret name in opensearch : ",secret_name)
DATABRICKS_TOKEN = get_secret(secret_name,key="databricks-token")
url = os.getenv("EMBEDDING_MODEL_URL")

print("DATABRICKS TOKEN FOR HEADER in opensearch file : ",DATABRICKS_TOKEN)
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}
print("headers for embedding model in openesarch file : ",headers)

OPENSEARCH_HOST_URL = os.getenv('OPENSEARCH_HOST_URL')
def get_opensearch_client():
    """Create a fresh OpenSearch client each time"""
    try:
        print("before initializing opensearch client")
        # Force fresh credentials by creating new session
        session = boto3.Session()
        
        open_search_client = wr.opensearch.connect(
            host=OPENSEARCH_HOST_URL,
            region="us-west-2",
            boto3_session=session , 
        )

        print("OpenSearch client initialized with host:", OPENSEARCH_HOST_URL)
        print("OpenSearch client : ",open_search_client)
        print("mapping of us-onc-iidd-genai__hcp index : ",open_search_client.indices.get_mapping(index=os.getenv("INDEX_NAME_BYOD")))

        if open_search_client:
            print("OpenSearch client initialized successfully.")
        else:
            print("Failed to initialize OpenSearch client.")

        return open_search_client
    except Exception as e:
        print(f"Error creating OpenSearch client: {e}")
        return None


index_name_hcp = os.getenv("INDEX_NAME_HCP")
index_name_byod = os.getenv("INDEX_NAME_BYOD")



class OpensearchExecutionManager:

    def __init__(self):
        """
        Initializes the execution manager with a loader.
        Args:
            hcp_loader (HCPFilesLoader): The loader for Gilead-related scripts.
        """
        print("init method of opensearch execution manager")
        
        # Configuration for LLM-based filtering for Physician Opinions v2
        self.enable_physician_opinions_filtering = os.getenv("ENABLE_PHYSICIAN_OPINIONS_FILTERING", "true").lower() == "true"
        print(f"LLM-based filtering for Physician Opinions v2 enabled: {self.enable_physician_opinions_filtering}")

        # print("Loaded loading_code_gilead successfully.")

    async def stream_on_ui(self, msg, text):
        """
        Streams a token to the UI.
        """
        await msg.send()
        for token in text.split():
            await msg.stream_token(token + " ")
            await asyncio.sleep(0.1)  # Simulate a delay for streaming
        await msg.update()


    def get_batch_embeddings(self, texts, max_workers=33):
        print("texts length:", len(texts))
        """Process multiple texts in parallel using ThreadPoolExecutor while preserving order"""
        try:
            print("inside get batch embeddings")
            # Ensure texts is a list of strings
            if not isinstance(texts, list) or not all(isinstance(text, str) for text in texts):
                raise ValueError("Input must be a list of strings.")
            embeddings = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Map preserves the original order of inputs
                print("inside executor map get batch embeddings")
                results = list(executor.map(self.get_single_embedding, texts))
                
                # Process results (which are now in the same order as texts)
                for embedding in results:
                    if isinstance(embedding, list):  # Only add valid embeddings
                        embeddings.append(embedding)
                    else:
                        embeddings.append({"error": embedding.get("error", "Unknown error")})
            
            return embeddings
        except Exception as e:
            print(f"Error in get_batch_embeddings: {e}")

    def get_single_embedding(self,text):
        """Process a single text and return its embedding"""
        try:
            # Ensure text is a string
            print("url for embedding model byod : ",url)
            if not isinstance(text, str):
                raise ValueError("Input must be a string.")
            print("inside get single embedding")
            payload = {
                "input": [text]  # Keeping the list format as in your original code
            }
            try:
                response = requests.post(url, json=payload, headers=headers)
                response_data = response.json()
                # Extract the embedding from the response
                embedding = response_data.get('data')[0]['embedding']
                return embedding
            except Exception as e:
                return {"error": str(e)}
        except Exception as e:
            print(f"Error in get_single_embedding: {e}")

    def get_single_embedding_pmr(self,text):
        """Process a single text and return its embedding"""
        try:
            # Ensure text is a string
            pmr_url=os.getenv("PMR_EMBEDDING_MODEL_URL")
            print("url for embedding model pmr : ",pmr_url)
            print("url for pmr : ",pmr_url)
            if not isinstance(text, str):
                raise ValueError("Input must be a string.")
            print("inside get single embedding")
            payload = {
                "input": [text]  # Keeping the list format as in your original code
            }
            try:
                response = requests.post(pmr_url, json=payload, headers=headers)
                response_data = response.json()
                # Extract the embedding from the response
                embedding = response_data.get('data')[0]['embedding']
                return embedding
            except Exception as e:
                return {"error": str(e)}
        except Exception as e:
            print(f"Error in get_single_embedding: {e}")

    async def execute_hybrid_search(self, question, search_query):

        print("inside execute hybrid search query")
        open_search_client = get_opensearch_client()

        if open_search_client is None:
            print("OpenSearch client is not initialized.")
        else:  # Return empty DataFrame if client is not initialized
            try:
                # Get embeddings for the question
                embeddings = self.get_single_embedding(question)
                # print("generated embeddings for the question",embeddings)
                query_body = eval(str(search_query))
                print("query body before qurying opensearch : ",query_body)
                # print("before printing query from llm ")
                # print("generated the search query ",query_body)
                
                # Execute the search
                # print("query body before requesting opensearch : ",query_body)
                response = open_search_client.search(
                    index=index_name_hcp, 
                    body=query_body
                )
                
                # Print results
                print("QUESTION ASKED:", question)
                print(f"Query executed successfully. {len(response['hits']['hits'])} results found.")
                
                # Convert results to a pandas DataFrame
                results_list = []
                for hit in response['hits']['hits']:
                    hit_data = hit["_source"]
                    hit_data['_score'] = hit["_score"]  # Add the score to the data
                    results_list.append(hit_data)
                
                # Create DataFrame from the list of dictionaries
                results_df = pd.DataFrame(results_list)
                
                # # Print the original format for reference
                # for hit in response['hits']['hits']:
                #     print("\nDocument Score:", hit["_score"])
                #     for key, value in hit["_source"].items():
                #         print(f"{key}: {value}")
                results_json = results_df.to_json(orient='records')
                print("results from opensearch index: ",results_json)
                return results_json
            
            except Exception as e:
                print(f"Error executing query: {e}")
                return pd.DataFrame()  # Return empty DataFrame in case of error
    
    async def execute_sql_on_opensearch(self,question,sql_query):
        open_search_client = get_opensearch_client()
        if open_search_client is None:
            print("OpenSearch client is not initialized. in excute sql on opensearch")
            return pd.DataFrame()
          # Return empty DataFrame if client is not initialized
        else:
            sql_query = {
                "query": sql_query,
            }
            print("inside execute sql query question asked : ",question)
            try:
                # Execute the query
                response = open_search_client.transport.perform_request('POST', '/_plugins/_sql', body=sql_query)
                
                # Handle different response formats
                if isinstance(response, dict):
                    # Standard response with schema and datarows
                    if 'schema' in response and 'datarows' in response:
                        columns = [col.get('alias', col.get('name', f'col_{i}')) 
                                for i, col in enumerate(response['schema'])]
                        df = pd.DataFrame(response['datarows'], columns=columns)
                        results_json = df.to_json(orient='records')
                        return results_json
                        # return df
                    
                    # Aggregate query response
                    elif 'aggregations' in response:
                        return pd.DataFrame.from_dict(response['aggregations'], orient='index')
                    
                    # Handle error responses
                    elif 'error' in response:
                        print(f"Error: {response['error']}")
                        return pd.DataFrame()
                    
                    # Any other dictionary response
                    else:
                        # Try to flatten the dictionary for DataFrame conversion
                        return pd.json_normalize(response)
                
                # Handle list responses
                elif isinstance(response, list):
                    return pd.DataFrame(response)
                
                # For any other type of response
                else:
                    return pd.DataFrame([{'result': response}])
            
            except Exception as e:
                print(f"Error executing query: {str(e)}")
                return pd.DataFrame()
            
    async def ingest_data_to_opensearch(self, user_thread, chunks, embeddings, index_name):

        open_search_client = get_opensearch_client()
        if open_search_client is None:
            print("OpenSearch client is not initialized. Cannot ingest data.")
            return 0, 0  # Return counts of 0 for success and failure
        else:
            cl.user_session.set("ingested_flag","true")  # Default to 5 seconds if not set
            await self.stream_on_ui(cl.Message(content=""),f"Ingesting {len(chunks)} chunks of data into OpenSearch...")
            
            # Prepare bulk request body
            bulk_data = []
            for i, (text, embedding) in enumerate(zip(chunks, embeddings)):
                # Action metadata - add routing parameter here
                bulk_data.append({
                    "index": {
                        "_index": index_name, 
                        "_id": user_thread + "_" + str(i),
                        "routing": user_thread  # Add routing parameter using user_thread
                    }
                })
                # Document data

                bulk_data.append({"text": text, "user_thread": user_thread, "embedding": embedding})

            # Convert to newline-delimited JSON
            bulk_body = "\n".join([json.dumps(item) for item in bulk_data]) + "\n"
            print("bulk body : ",bulk_body)

            # Send the bulk request
            response = open_search_client.bulk(body=bulk_body)

            # Check for errors
            if response["errors"]:
                print("There were errors in the bulk operation")
                for item in response["items"]:
                    if "error" in item["index"]:
                        print(f"Error for item {item['index']['_id']}: {item['index']['error']}")
            else:
                success_count = len(response["items"])
                failed_count = 0
                print(f"Successfully inserted {len(response['items'])} Chunks into OpenSearch")
                # await self.stream_on_ui(cl.Message(content=""),f"Successfully inserted {len(response['items'])} Chunks into OpenSearch")
            


            return success_count, failed_count
        
    def normalize_embedding(self,embedding):
        """
        Normalize embedding vector to unit length for cosine similarity
        """
        # Convert to numpy array
        embedding_array = np.array(embedding, dtype=np.float32)
        
        # Calculate L2 norm (magnitude)
        norm = np.linalg.norm(embedding_array)
        
        # Avoid division by zero
        if norm == 0:
            return embedding
        
        # Normalize to unit length
        normalized = embedding_array / norm
        
        # Convert back to list
        return normalized.tolist()

    async def pmr_hybrid_search(self, user_question, hybrid_query_pmr):
        """Execute hybrid search with corrected doc_type."""
        
        max_attempts = 3
        for attempt in range(max_attempts):

            print(f"Attempt {attempt + 1} of {max_attempts}")
            # QUICK FIX: Update doc_type in query
            hybrid_query = copy.deepcopy(hybrid_query_pmr)
            hybrid_query['query']['bool']['must'][0]['term']['doc_type'] = 'pmr_pii_cosine_with_profiles_updated'
            
            # Get and inject embedding
            print("USER QUESTION IN PMR HYBRID SEARCH: ",user_question)
            question_embedding = self.get_single_embedding_pmr(user_question)
            hybrid_query['query']['bool']['must'][1]['knn']['pmr_embedding_pii_removal_profile_cosine_updated']['vector'] = self.normalize_embedding(question_embedding)
            
            print(f"✓ Embedding injected: {len(question_embedding)} dims")
            print(f"✓ Filters: {len(hybrid_query['query']['bool'].get('filter', []))}")

            print("final query from pmr to opensearch: ",hybrid_query)
            
            # Execute search with corrected routing
            open_search_client = get_opensearch_client()
            response = open_search_client.search(
                index=index_name_byod,
                body=hybrid_query,
                routing="pmr_pii_cosine_with_profiles_updated"  # Corrected
            )
            
            total_hits = response['hits']['total']['value']
            print(f"✓ Results: {total_hits} hits")
            if total_hits>0:
                
                # Process results
                # text_results = {"questions": [], "answers": [], "respondent_type": [],'source':[]}
                text_results={'questions':[], 'answers':[],'respondent_type':[],
                'source':[], 'tier':[], 'medical_profession':[], 'primary_medical_specialty':[],
                'practice_setting':[], 'geography_setting':[], 'identify_as':[],
                'tro_user/non_user':[], 'age':[], 'gender':[], 'health_insurance_coverage':[],'years_in_practice':[],'area_of_treatment':[]}
                for hit in response["hits"]["hits"]:
                    text_results['questions'].append(hit["_source"].get("pmr_question_pii_removal_profile_cosine_updated", ""))
                    text_results['answers'].append(hit["_source"].get("pmr_answer_pii_removal_profile_cosine_updated", ""))
                    text_results['respondent_type'].append(hit["_source"].get("pmr_respondent_type_pii_removal_profile_cosine_updated", ""))
                    text_results['source'].append(hit["_source"].get("pmr_source_pii_removal_profile_cosine_updated", ""))
                    text_results['tier'].append(hit["_source"].get("pmr_tier_pii_removal_profile_cosine_updated", ""))
                    text_results['medical_profession'].append(hit["_source"].get("pmr_medical_profession_pii_removal_profile_cosine_updated", ""))
                    text_results['primary_medical_specialty'].append(hit["_source"].get("pmr_primary_medical_specialty_pii_removal_profile_cosine_updated", ""))
                    text_results['practice_setting'].append(hit["_source"].get("pmr_practice_setting_pii_removal_profile_cosine_updated", ""))
                    text_results['geography_setting'].append(hit["_source"].get("pmr_geography_setting_pii_removal_profile_cosine_updated", ""))
                    text_results['identify_as'].append(hit["_source"].get("pmr_identify_as_pii_removal_profile_cosine_updated", ""))
                    tro_user_or_not=hit["_source"].get("pmr_tro_user/non_user_pii_removal_profile_cosine_updated", "")
                    if tro_user_or_not=="Dabbler" or tro_user_or_not=="Adopter" or tro_user_or_not=="User":
                        tro_user_or_not="TRO User"
                    else:
                        tro_user_or_not="Non-TRO User"
                    text_results['tro_user/non_user'].append(tro_user_or_not)
                    text_results['age'].append(hit["_source"].get("pmr_age_pii_removal_profile_cosine_updated", ""))
                    text_results['gender'].append(hit["_source"].get("pmr_gender_pii_removal_profile_cosine_updated", ""))
                    text_results['health_insurance_coverage'].append(hit["_source"].get("pmr_health_insurance_coverage_pii_removal_profile_cosine_updated", ""))
                    text_results['years_in_practice'].append(hit["_source"].get("pmr_years_in_practice_pii_removal_profile_cosine_updated", ""))
                    text_results['area_of_treatment'].append(hit["_source"].get("pmr_area_of_treament_pii_removal_profile_cosine_updated", ""))
                    
                return text_results
            
            else:
                pass 

    async def market_map_hybrid_search(self, user_question, hybrid_query_mrkt_map):

        """Execute hybrid search with corrected doc_type."""
        
        # QUICK FIX: Update doc_type in query
        hybrid_query=hybrid_query_mrkt_map
        print("hybrid query for mrkt map:", hybrid_query)
        hybrid_query['query']['bool']['must'][0]['term']['doc_type'] = 'mrkt_map_l2'
        
        # Get and inject embedding
        question_embedding = self.get_single_embedding_pmr(user_question)
        hybrid_query['query']['bool']['must'][1]['knn']['mrkt_map_embedding']['vector'] = question_embedding
        
        print(f"✓ Embedding injected: {len(question_embedding)} dims")
        print(f"✓ Filters: {len(hybrid_query['query']['bool'].get('filter', []))}")

        print("final query from pmr to opensearch: ",hybrid_query)
        
        # Execute search with corrected routing
        open_search_client = get_opensearch_client()
        response = open_search_client.search(
            index=index_name_byod,
            body=hybrid_query,
            routing="mrkt_map_l2"  # Corrected
        )
        
        total_hits = response['hits']['total']['value']
        print(f"✓ Results: {total_hits} hits")
        
        # Process results
        # text_results = {"questions": [], "answers": [], "respondent_type": [],'source':[]}
        text_results={'questions':[], 'answers':[],'respondent_type':[],
       'source':[], 'indication':[]}
        for hit in response["hits"]["hits"]:
            text_results['questions'].append(hit["_source"].get("mrkt_map_question", ""))
            text_results['answers'].append(hit["_source"].get("mrkt_map_answer", ""))
            text_results['respondent_type'].append(hit["_source"].get("mrkt_map_respondent", ""))
            text_results['source'].append(hit["_source"].get("mrkt_map_source", ""))
            text_results['indication'].append(hit["_source"].get("mrkt_map_indication", ""))
        return text_results
    
    async def early_exp_hybrid_search(self, user_question, hybrid_query_early):

        """Execute hybrid search with corrected doc_type."""
        
        # QUICK FIX: Update doc_type in query
        hybrid_query=hybrid_query_early
        print("hybrid query for mrkt map:", hybrid_query)
        hybrid_query['query']['bool']['must'][0]['term']['doc_type'] = 'init_expr_updated_v2'
        
        # Get and inject embedding
        question_embedding = self.get_single_embedding_pmr(user_question)
        hybrid_query['query']['bool']['must'][1]['knn']['init_expr_embedding_v2_ingest']['vector'] = question_embedding
        
        print(f"✓ Embedding injected: {len(question_embedding)} dims")
        print(f"✓ Filters: {len(hybrid_query['query']['bool'].get('filter', []))}")

        print("final query from pmr to opensearch: ",hybrid_query)
        
        # Execute search with corrected routing
        open_search_client = get_opensearch_client()
        response = open_search_client.search(
            index=index_name_byod,
            body=hybrid_query,
            routing="init_expr_updated_v2"  # Corrected
        )
        
        total_hits = response['hits']['total']['value']
        print(f"✓ Results: {total_hits} hits")
        
        # Process results
        # text_results = {"questions": [], "answers": [], "respondent_type": [],'source':[]}
        text_results={'questions':[], 'answers':[],'respondent_type':[],
       'source':[]}
        for hit in response["hits"]["hits"]:
            text_results['questions'].append(hit["_source"].get("init_expr_question_v2_ingest", ""))
            text_results['answers'].append(hit["_source"].get("init_expr_answer_v2_ingest", ""))
            text_results['respondent_type'].append(hit["_source"].get("init_expr_respondent_type_v2_ingest", ""))
            text_results['source'].append(hit["_source"].get("init_expr_source_v2_ingest", ""))
        return text_results

    async def pbc_market_research_hybrid_search(self, user_question, hybrid_query_mr):

        """Execute hybrid search with pbc market research doc type."""
        
        # QUICK FIX: Update doc_type in query
        hybrid_query=hybrid_query_mr
        print("hybrid query for pbc mr:", hybrid_query)
        hybrid_query['query']['bool']['must'][0]['term']['doc_type'] = 'pbc_market_research'
        
        # Get and inject embedding
        question_embedding = self.get_single_embedding_pmr(user_question)
        old_embedding_field = list(hybrid_query['query']['bool']['must'][1]['knn'].keys())[0] 
        print("old embedding field: ",old_embedding_field)
        hybrid_query['query']['bool']['must'][1]['knn']['pbc_market_research_embedding']['vector'] = question_embedding
        
        print(f"✓ Embedding injected: {len(question_embedding)} dims")
        print(f"✓ Filters: {len(hybrid_query['query']['bool'].get('filter', []))}")

        print("final query from pbc to opensearch: ",hybrid_query)
        
        # Execute search with corrected routing
        open_search_client = get_opensearch_client()
        response = open_search_client.search(
            index=index_name_byod,
            body=hybrid_query,
            routing="pbc_market_research"  # Corrected
        )
        
        total_hits = response['hits']['total']['value']
        print(f"✓ Results: {total_hits} hits")
        if total_hits>0:
            
            # Process results
            # text_results = {"questions": [], "answers": [], "respondent_type": [],'source':[]}
            # PBC-specific results structure
            text_results = {'questions': [],'answers': [],'respondent_type': [],'source': [],'primary_specialty': [],'practice_setting': [],'practice_state': [],
                'years_in_practice': [],'pct_patient_care': [],'pct_research_teaching': [],'pct_administrative': [],'unique_patients': []}
            
            for hit in response["hits"]["hits"]:
                text_results['questions'].append(hit["_source"].get("pbc_market_research_question", ""))
                text_results['answers'].append(hit["_source"].get("pbc_market_research_answer", ""))
                text_results['respondent_type'].append(hit["_source"].get("pbc_market_research_respondent_type", ""))
                text_results['source'].append(hit["_source"].get("pbc_market_research_source", ""))
                text_results['primary_specialty'].append(hit["_source"].get("pbc_market_research_primary_specialty", ""))
                text_results['practice_setting'].append(hit["_source"].get("pbc_market_research_practice_setting", ""))
                text_results['practice_state'].append(hit["_source"].get("pbc_market_research_practice_state", ""))
                text_results['years_in_practice'].append(hit["_source"].get("pbc_market_research_years_in_practice", ""))
                text_results['pct_patient_care'].append(hit["_source"].get("pbc_market_research_pct_patient_care", ""))
                text_results['pct_research_teaching'].append(hit["_source"].get("pbc_market_research_pct_research_teaching", ""))
                text_results['pct_administrative'].append(hit["_source"].get("pbc_market_research_pct_administrative", ""))
                text_results['unique_patients'].append(hit["_source"].get("pbc_market_research_unique_patients", ""))
                
            return text_results
        
        else:
            pass 
       

    
    async def byod_hybrid_search(self, user_thread, question_embedding, sleep_time = 2,category=None):
        """
        Perform a hybrid search and return only the text from the top 5 results.
        
        Args:
            user_thread (str): The text query to match against the text field
            question_embedding (list): The vector embedding of the query
            open_search_client: The OpenSearch client instance
            
        Returns:
            list: A list containing only the text content from the top 5 results
        """
        open_search_client = get_opensearch_client()
        if open_search_client is None:
            print("OpenSearch client is not initialized. Cannot perform hybrid search.")
            return []
        else:
            print("inside byod hybrid search : user_thread ",user_thread," question_embedding : ",question_embedding)
            # Define the hybrid query combining text and vector search
            hybrid_query = { 
                "query": {
                    "bool": {
                        "must": [
                            # Text-based search component using the "text" field
                            {
                                "match_phrase": {
                                    "user_thread": user_thread
                                }
                            },
                            # Vector similarity search component
                            {
                                "knn": {
                                    "embedding": {
                                        "vector": question_embedding[0],
                                        "k": 10000
                                    }
                                }
                            }
                        ]
                    }
                },
                # Only return the text field to minimize response size
                "_source": ["text"],
                "size": 200, 
            }
            # Execute the search
            if cl.user_session.get("chat_profile")=="PMR":
                hybrid_query={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"doc_type": "pmr_pii_L2"}},
                                {"knn": {"pmr_embedding_pii_L2": {"vector": question_embedding[0], "k": 10000}}}
                            ]
                        }
                    },
                    "_source": ["pmr_question_pii_L2", "pmr_answer_pii_L2", "pmr_combined_pii_L2"],
                    "size": 100,
                }
            if cl.user_session.get("chat_profile")=="Physician_opinions_v2":
                hybrid_query={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"doc_type": "PO"}},
                                {"knn": {"PO_embeddings": {"vector": question_embedding[0], "k": 10000}}}
                            ]
                        }
                    },
                    "_source": ["PO_postid", "PO_posts"],
                    "size": 100,
                }
            print("hybrid query : ",hybrid_query) 

            ingested_flag = cl.user_session.get("ingested_flag","")
            if ingested_flag == "true":
                msg = cl.Message(content="")
                text = "Please wait as the ingestion is in process \n waiting for approxmately " + str(sleep_time) + " seconds"
                await self.stream_on_ui(msg,text)
                for i in range(1, sleep_time + 1):
                    await asyncio.sleep(1)
                    msg.content=f"{text} {'..' * i}"
                    await msg.update()
                cl.user_session.set("ingested_flag","false") 
                msg.content = ""
                await self.stream_on_ui(msg,"Ingestion is complete.") # Reset the flag after waiting

            msg = cl.Message(content="")        
            await self.stream_on_ui(msg,"Fetching relevant chunks from OpenSearch...")

            
            if cl.user_session.get("chat_profile")=="PMR":

                response = open_search_client.search(
                index=index_name_byod,
                body=hybrid_query,
                routing="pmr_pii_L2"
                )
                text_results={"questions":[],"answers":[]}
                for hit in response["hits"]["hits"]:
                    question = hit["_source"].get("pmr_question_pii_L2", "")
                    answer = hit["_source"].get("pmr_answer_pii_L2", "")
                    combined = hit["_source"].get("pmr_combined_pii_L2", "")
                    text_results['questions'].append(question)
                    text_results['answers'].append(answer)
                    # text_results['combined'].append(combined)
                return text_results
            
            if cl.user_session.get("chat_profile")=="Physician_opinions_v2":
                print("inside physician opinions v2 hybrid search")
                response = open_search_client.search(
                index=index_name_byod,
                body=hybrid_query,
                )
                text_results={"unique_id":[],"Posts":[]}
                for hit in response["hits"]["hits"]:
                    question = hit["_source"].get("PO_postid", "")
                    answer = hit["_source"].get("PO_posts", "")
                    text_results['unique_id'].append(question)
                    text_results['Posts'].append(answer)
                return text_results
            
            response = open_search_client.search(
                index=index_name_byod,
                body=hybrid_query,
                routing=user_thread 
            )            
            text_results = " ".join([hit["_source"]["text"] for hit in response["hits"]["hits"]])
            print("generated the text results from opensearch : ",text_results)
            return text_results
        
    async def get_relevant_chunks_from_opensearch(self, user_id, thread_id, question,category=None):
        """
        Fetch relevant chunks from OpenSearch based on the user ID, thread ID, and question.
        """
        print("inside get relevant chunks from opensearch")
        open_search_client = get_opensearch_client()
        if open_search_client is None:
            print("OpenSearch client is not initialized. Cannot fetch relevant chunks.")
            return []
        else:
            try:
                user_thread = user_id + "_" + thread_id
                print("Fetching relevant chunks from OpenSearch...")
                print("question from user to opensearch: ",question)
                question_embedding = [self.get_single_embedding(question) if cl.user_session.get("chat_profile")!="PMR" and cl.user_session.get("chat_profile")!="Physician_opinions_v2" else self.get_single_embedding_pmr(question) ]
                print("Generated embeddings for the question ",len(question_embedding))
                # time.sleep(2)
                sleep_time = cl.user_session.get("sleep_time")
                results = await self.byod_hybrid_search(user_thread, question_embedding, sleep_time) if cl.user_session.get("chat_profile")!="PMR" else await self.byod_hybrid_search(user_thread, question_embedding, sleep_time,category=category)
                print(f"Fetched relevant chunks.\nAnswering your question: {question}")
                await self.stream_on_ui(cl.Message(content=""),f"Fetched relevant chunks.\n Let me answer your question ")
                return results
            
            except Exception as e:
                print(f"Error fetching relevant chunks: {e}")
                return []

#     async def filter_physician_opinions_data_with_llm(self, opensearch_results, user_question):
#         """
#         Use LLM to filter and select the most relevant Physician Opinions data from OpenSearch results.
        
#         Args:
#             opensearch_results: Raw results from OpenSearch (dict with 'unique_id' and 'Posts' lists)
#             user_question: The user's original question
            
#         Returns:
#             Filtered and relevant data for answering the user's question
#         """
#         try:
#             print("Starting LLM-based filtering for Physician Opinions v2...")
#             await self.stream_on_ui(cl.Message(content=""), "Filtering relevant physician opinions using AI...")
            
#             # Import LLM client here to avoid circular imports
#             from Structured_Bot.llm_requests_new import LLM
#             llm_client = LLM()
            
#             # Create the filtering prompt for Physician Opinions
#             filtering_prompt = """You are a data filtering specialist for Physician Opinions data. Your task is to analyze OpenSearch results and extract only the most relevant physician posts to answer the user's question.

# Your filtering criteria:
# 1. Focus on posts that directly address the user's question topic
# 2. Prioritize posts with clear opinions, insights, or clinical information
# 3. Exclude posts that are too brief, unclear, or off-topic
# 4. Maintain the post structure with unique IDs for reference
# 5. If multiple posts cover similar topics, select the most informative or recent ones

# Output format:
# - Return a JSON object with the same structure as input: {"unique_id": [...], "Posts": [...]}
# - Include only the most relevant posts
# - If no relevant data is found, return {"unique_id": [], "Posts": []}

# Be selective and focus on posts that provide meaningful insights to answer the user's question."""

#             # Prepare the data for LLM analysis
#             data_summary = f"Total posts retrieved: {len(opensearch_results.get('Posts', []))}\n"
#             data_summary += f"Sample posts: {str(opensearch_results)[:4000]}"  # Limit input size
            
#             # Call LLM for filtering
#             messages = [
#                 {"role": "system", "content": filtering_prompt},
#                 {"role": "user", "content": f"User Question: {user_question}\n\nOpenSearch Results: {data_summary}"}
#             ]
            
#             # Get LLM response for filtering
#             response = await llm_client.send_request_streaming(messages, tools=[])
            
#             filtered_content = ""
#             async for stream_resp in response:
#                 if stream_resp.choices[0].delta.content:
#                     filtered_content += stream_resp.choices[0].delta.content
            
#             print("LLM filtering completed for Physician Opinions")
#             await self.stream_on_ui(cl.Message(content=""), "Data filtering completed. Using most relevant physician opinions...")
            
#             # Try to parse the filtered JSON response
#             try:
#                 import json
#                 filtered_data = json.loads(filtered_content.strip())
#                 if isinstance(filtered_data, dict) and 'unique_id' in filtered_data and 'Posts' in filtered_data:
#                     print(f"Successfully filtered data: {len(filtered_data['Posts'])} posts selected from {len(opensearch_results.get('Posts', []))} original posts")
#                     return filtered_data
#                 else:
#                     print("LLM returned invalid JSON structure, using original results")
#                     return opensearch_results
#             except json.JSONDecodeError:
#                 print("LLM returned invalid JSON, using original results")
#                 return opensearch_results
                
#         except Exception as e:
#             print(f"Error in LLM-based filtering for Physician Opinions: {e}")
#             # Fallback to original results if filtering fails
#             return opensearch_results

    