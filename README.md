Hi Technical Reviewer/s
My name is Franco Tacchella and this is my completed techincal challenge. I want to first state that this was a robust project and in my opinion not the best way to evaluate a candidate as the most important thing in data engineering is probably a deep understanding of the information relevant users need. 
As a suggestion, giving candidates a data set and seeing how the build schemas and tables is probably a better aproach. 
If you read this I thank you and you will now find the documentation of my project. I really hope I am considered for this position.


# SEC Filings Data Engineering & Search API
## Objective
This project implements a complete, end-to-end data engineering solution to ingest, process, and serve financial data. The system features a multi-stage ETL pipeline based on the Medallion Architecture, a relational data warehouse using a Star Schema, and a powerful semantic search capability provided by a vector database. The entire system is exposed through a secure, containerized REST API.

## Key Features
### End-to-End ETL Pipeline: 
A full Bronze, Silver, and Gold data pipeline to process raw data into a query-ready state.

### Dimensional Data Warehouse:
A SQL database modeled with a 5-dimension Star Schema for complex analytical queries.

### Semantic Vector Search:
Fast and relevant search over filing summaries using vector embeddings.

### Full CRUD API:
Endpoints to Create, Read, Update, and Delete raw data records.

### Layered Architecture: 
The code is structured into distinct layers for API routing, services (business logic), domain entities, and data access.

### Containerized Environment: 
The API and database services are fully containerized with Docker for easy and reliable deployment.

### Historical Data Tracking:
Implements a Slowly Changing Dimension (SCD) Type 2 on the Company dimension to preserve a full history of changes.

## Tech Stack
Backend: Python 3.11, FastAPI
Databases: Typesense (Vector Search), SQLite (Data Warehouse)
ETL & Data Handling: Pandas, SQLModel (ORM)
AI / Embeddings: sentence-transformers
Synthetic Data: sdv, Faker
Containerization: Docker, Docker Compose

## Getting Started
Follow these steps to set up and run the project locally.

### 1. Clone the Repository
$ git clone <your-repository-url>
$ cd <your-repository-name>

### 2. Set Up the Python Environment
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r api/requirements.txt
3. Start the Docker Services
This command will build the API image and start the FastAPI and Typesense containers.
$ docker-compose up -d --build


### 3. Running the Full Data Pipeline
After the services are running, execute the following scripts in order from the project's root directory to generate and process all data.

3.1. Generate raw data (Bronze Layer)
$ python generate_sdv_data.py

3.2. Process raw data into the clean layer (Silver Layer)
$ python etl/bronze_to_silver.py

3.3. Create a fresh, empty data warehouse schema (Gold Layer)
$ python create_db.py

3.4. Load the clean data into the data warehouse
$ python -m etl.silver_to_gold

3.5. Ingest data and embeddings into the Typesense search index
$ python ingest_to_typesense.py
After these scripts complete, the system is fully populated and ready to use.

### API Usage
The interactive API documentation is the best way to explore the endpoints.

Swagger UI: http://localhost:8000/docs

Authentication
All API endpoints are protected with Basic Authentication. Use the "Authorize" button in the API docs with the default credentials:

Username: admin

Password: supersecret

Example cURL Commands
Vector Search (Semantic Search)
Search for filings related to "risk and growth".

Bash

$ curl -X GET "http://localhost:8000/search?q=risk%20and%20growth" -u "admin:supersecret"
Analytical Query (SQL Data Warehouse)
Get the top 5 companies by total reported financial value.

Bash

$ curl -X GET "http://localhost:8000/query/company-totals?limit=5" -u "admin:supersecret"
CRUD: Create a New Raw Record

Bash

$ curl -X 'POST' \
  'http://localhost:8000/raw/submissions/' \
  -u "admin:supersecret" \
  -H 'Content-Type: application/json' \
  -d '[
    {
      "adsh": "test-crud-001",
      "cik": 9876543,
      "name": "CRUD Test Corp",
      "form": "API-TEST",
      "sic": 1111,
      "filing_summary": "Testing the create endpoint."
    }
  ]'
CRUD: Get a Specific Raw Record

Bash

$ curl -X GET "http://localhost:8000/raw/submissions/test-crud-001" -u "admin:supersecret"
