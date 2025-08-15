import os
import typesense
from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Optional, Dict, Any

# --- Setup and Configuration ---
# Typesense client connection to the Docker container
try:
    client = typesense.Client({
        'nodes': [{
            'host': os.environ.get('TYPESENSE_HOST', 'typesense'),
            'port': '8108',
            'protocol': 'http'
        }],
        'api_key': os.environ.get('TYPESENSE_API_KEY', 'xyz'),
        'connection_timeout_seconds': 2
    })
    
    # Verify the connection
    client.collections.retrieve()
    print("Successfully connected to Typesense server.")
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Error connecting to Typesense: {e}")

# Define the FastAPI application and security scheme
app = FastAPI(title="Financial Filings Search API")
security = HTTPBasic()

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = os.environ.get("API_USERNAME")
    correct_password = os.environ.get("API_PASSWORD")
    
    if credentials.username != correct_username or credentials.password != correct_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"message": "Welcome to the Financial Filings API."}

@app.get("/search", response_model=Dict[str, Any])
def search_filings(
    query: str = Query(..., description="The search query."),
    company_name: Optional[str] = Query(None, description="Filter results by company name."),
    username: str = Depends(get_current_username)
):
    """
    Search for financial filings using a natural language query with optional filters.
    This endpoint requires authentication.
    """
    
    search_parameters = {
        'q': query,
        'query_by': 'company_name,content',
        'per_page': 10,
    }

    if company_name:
        search_parameters['filter_by'] = f"company_name:={company_name}"
    
    try:
        results = client.collections['financial_filings'].documents.search(search_parameters)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")