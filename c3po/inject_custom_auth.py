# inject_custom_auth.py
import os
import secrets
import string
from chainlit.oauth_providers import providers

def databricks_oauth_enabled():
    if (os.environ.get('DATABRICKS_CLIENT_ID') is not None and 
        os.environ.get('DATABRICKS_CLIENT_SECRET') is not None):
        print("Databricks OAuth configured.")
        return True
    else:
        print("Databricks OAuth not configured. Skipping...")
        return False

def provider_id_in_instance_list(provider_id: str):
    if providers is None:
        print("No providers found")
        return False
    if not any(provider.id == provider_id for provider in providers):
        print(f"Provider {provider_id} not found")
        return False
    else:
        print(f"Provider {provider_id} found")
        return True

def add_custom_oauth_provider(provider_id: str, custom_provider_instance):
    if databricks_oauth_enabled() and not provider_id_in_instance_list(provider_id):
        providers.append(custom_provider_instance)
        print(f"Added provider: {provider_id}")
    else:
        print("Provider details: ",provider_id)
        print("Provider custom provider:",custom_provider_instance)
        print(f"Custom OAuth is not enabled or provider {provider_id} already exists")