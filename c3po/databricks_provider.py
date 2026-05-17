# databricks_provider.py
import os
import httpx
from typing import Dict, Tuple, Optional
from chainlit.user import User
from chainlit.oauth_providers import OAuthProvider
import uuid
import hashlib
import base64
from chainlit.oauth_providers import providers
import jwt
import requests
# providers.append(DatabricksProvider())


class DatabricksProvider(OAuthProvider):
    id = "databricks"
    env = ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_WORKSPACE_URL", "DATABRICKS_REDIRECT_URI"]
    # These will be populated in __init__
    authorize_url = None
    token_url = None
    
    def __init__(self):
        # Get configuration from environment variables
        DATABRICKS_WORKSPACE_URL="https://dbc-5c6e6e7d-7beb.cloud.databricks.com"
        DATABRICKS_REDIRECT_URI="https://onc.c3podev.gilead.com/auth/oauth/databricks/callback"

        self.client_id = os.environ.get("DATABRICKS_CLIENT_ID")
        self.client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
        self.workspace_url = DATABRICKS_WORKSPACE_URL
        self.redirect_uri = DATABRICKS_REDIRECT_URI
        self.authorize_url = f"{self.workspace_url}/oidc/v1/authorize"
        self.token_url = f"{self.workspace_url}/oidc/v1/token"
        # Generate PKCE code verifier and challenge
        self.code_verifier = self._generate_code_verifier()
        self.code_challenge = self._generate_code_challenge(self.code_verifier)
        self.refresh_token=None
        self.redirect_after_replace=None
    
    def _generate_code_verifier(self) -> str:
        """Generate a PKCE code verifier"""
        uuid1 = uuid.uuid4()
        uuid_str1 = str(uuid1).upper()
        return uuid_str1 + "-" + uuid_str1
    
    def _generate_code_challenge(self, verifier: str) -> str:
        """Generate a PKCE code challenge from a verifier"""
        code_challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode('utf-8')
        return code_challenge.replace('=', '')
    
    # async def get_authorize_url(self, redirect_uri: str) -> str:
    #     """Override the default method to include code_challenge for PKCE"""
    #     params = {
    #         "client_id": self.client_id,
    #         "redirect_uri": redirect_uri,
    #         "response_type": "code",
    #         "scope": "offline_access sql",
    #         "code_challenge": self.code_challenge,
    #         "code_challenge_method": "S256"
    #     }
    # query = "&".join([f"{k}={v}" for k, v in params.items()])
    #         return f"{self.authorize_url}?{query}"

    def _generate_state(self) -> str:
        """Generate a secure random state parameter"""
        import secrets
        return secrets.token_urlsafe(32)
    
    async def get_authorize_url(self, redirect_uri: str) -> str:
        """Generate authorization URL with PKCE code challenge"""

        params = dict(self.authorize_params)
        params["redirect_uri"] = redirect_uri
        
        query = "&".join([f"{k}={v}" for k, v in params.items()])

        print("query in get_authorization_url : ",f"{self.authorize_url}?{query}")
        
        return f"{self.authorize_url}?{query}"
               
    @property
    def authorize_params(self):
        """Return the authorization parameters for OAuth"""

        return {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": "offline_access+sql"
        }
    
    async def get_token(self, code: str, redirect_uri: str) -> str:
        """Exchange authorization code for access token"""
        try:
            print("In custom authentication flow")
            # print("self.client_id: ",self.client_id)
            # print("self.client_secret: ",self.client_secret)
            print("self.token_url: ",self.token_url)
            print("code: ",code)
            print("redirect_uri: ",redirect_uri)
            # print("self.code_verifier: ",self.code_verifier)
            if "https" not in redirect_uri:
                redirect_uri = redirect_uri.replace("http://", "https://")
            payload = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "all-apis offline_access"
            }
            self.redirect_after_replace=redirect_uri
            #print("code in custom provider",code)
            # async with httpx.AsyncClient() as client:
            #     response = await client.post(
            #         self.token_url,
            #         data=payload
            #     )
                
            #     if response.status_code != 200:
            #         print(f"Error getting token: {response.text}")
            #         raise Exception(f"Failed to get token: {response.status_code}")
                
            #     token_data = response.json()
            #     return token_data.get("access_token")
            resp=requests.post(self.token_url, data=payload)
            if resp.status_code != 200:
                print(f"Error getting token: {resp.text}")
                raise Exception(f"Failed to get token: {resp.status_code}")
            token_data = resp.json()
            access_token = token_data.get("access_token")
            self.refresh_token=token_data.get("refresh_token")
            return access_token
        except Exception as e:
            print(f"Exception in get_token: {str(e)}")
            raise e
        
    def new_token_from_refresh_token(self):
        try:
            payload = {
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "redirect_uri": self.redirect_after_replace,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": "offline_access sql"
                }
            resp=requests.post(self.token_url, data=payload)
            if resp.status_code != 200:
                print(f"Error getting token: {resp.text}")
                raise Exception(f"Failed to get token: {resp.status_code}")
            new_token_data = resp.json()
            new_access_token = new_token_data.get("access_token")
            print("new access token at new_token_from_refresh_token function: ",new_access_token)
            self.refresh_token=new_token_data.get("refresh_token")
            return new_access_token
        except Exception as e:
            print(f"Exception in new_token_from_refresh_token: {str(e)}")
            raise e
        

    
    async def get_user_info(self, token: str) -> Tuple[Dict, User]:
        """
        Get user info from Databricks
        
        Note: Databricks might not provide standard user info endpoints.
        We'll create a basic user object with a placeholder username.
        """
        # Create a basic user object
        # In a real implementation, you might want to call a Databricks API to get actual user info
        user_name = jwt.decode(token, options={"verify_signature": False}).get("sub")
        user_data = {
            "id": user_name,  # Placeholder
            "username": user_name,
            "provider": self.id,
            "access_token":token
        }
        
        user = User(
            identifier=user_data["id"],
            metadata={
                "username": user_data["username"],
                "provider": user_data["provider"],
                "access_token":user_data["access_token"]
            }
        )
        return user_data, user