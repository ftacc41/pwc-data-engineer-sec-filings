SEC Filings Data Engineering & Search API
This project is a comprehensive, end-to-end data engineering solution designed to ingest, process, and serve financial data from SEC filings. It implements a full ETL pipeline using a Medallion Architecture, stores the processed data in both a relational data warehouse and a vector database, and exposes the data through a secure, containerized REST API with advanced search capabilities.

Key Features
End-to-End ETL Pipeline: Implements a Bronze, Silver, and Gold Medallion Architecture to process raw data into a query-ready state.

Synthetic Data Generation: Utilizes the Synthetic Data Vault (sdv) library to create realistic, relational synthetic data, including unstructured text for PDF reports.

Dimensional Data Warehouse: The Gold layer is a SQL database with a 5-dimension Star Schema, optimized for analytical queries.

Semantic Vector Search: Leverages sentence-transformers and a Typesense vector database to provide fast and semantically relevant search over filing summaries.

Layered REST API: A secure FastAPI application provides endpoints for search, analytical queries, and raw data management.

Containerized Environment: The entire application stack (API and Vector DB) is managed with Docker and Docker Compose for easy setup and consistent deployment.

Architecture Overview
The system is designed around a modern data stack with a clear separation of concerns.

Data Lake (Bronze Layer): Raw, unstructured (PDF) and structured (CSV) data is generated and stored in the data/bronze directory.

Staging Layer (Silver Layer): The raw data is cleaned, text is extracted from PDFs, and the data is transformed into a standardized Parquet format in the data/silver directory.

Data Warehouse (Gold Layer): The cleaned data is loaded into a SQLite database (data/warehouse.db) following a Star Schema with one fact table and five dimension tables.

Vector Database: Textual data from the Silver Layer is converted into vector embeddings and ingested into a Typesense collection for fast nearest-neighbor search.

Detailed Mermaid diagrams for each layer can be found in the /diagrams directory.

Tech Stack
Category	Technology / Library
Language	Python 3.11
API Framework	FastAPI
Data Orchestration	Sequential Python Scripts
ETL Framework	Pandas
Data Warehouse	SQLite
ORM	SQLModel
Vector Database	Typesense
AI / Embeddings	sentence-transformers
Synthetic Data	sdv, Faker, fpdf2
Containerization	Docker, Docker Compose
API Schemas	Pydantic
Getting Started
Follow these instructions to set up and run the project locally.

Prerequisites
Python 3.9+

Docker and Docker Compose

1. Clone the Repository
Bash

git clone <your-repository-url>
cd <your-repository-name>
2. Set Up the Environment
Create and activate a Python virtual environment.

Bash

python3 -m venv .venv
source .venv/bin/activate
Install the required Python dependencies.

Bash

pip install -r api/requirements.txt
3. Start the Services
This command will build the FastAPI application image and start both the API and Typesense containers in the background.

Bash

docker-compose up -d --build
4. Run the Data Pipeline
Execute the following scripts in order from the project's root directory to generate data and populate the databases.

Bash

# 1. Generate raw data in the Bronze layer
python generate_sdv_data.py

# 2. Process Bronze data and save to the Silver layer
python etl/bronze_to_silver.py

# 3. Create a fresh, empty Gold layer database
python create_db.py

# 4. Load the Silver data into the Gold layer star schema
python -m etl.silver_to_gold

# 5. Ingest data and embeddings into the Typesense vector DB
python ingest_to_typesense.py
After these steps, the entire system is running and populated with data.

API Usage
The API is now running and accessible. The interactive documentation (Swagger UI) is the best way to explore the endpoints.

API Docs: http://localhost:8000/docs

Authentication
All endpoints are protected with Basic Authentication. Use the "Authorize" button in the API docs with the following default credentials (defined in docker-compose.yml):

Username: admin

Password: supersecret

Example Endpoints
Vector Search
Performs a semantic search for the term "risk".

Bash

curl -X GET "http://localhost:8000/search?q=risk" -u "admin:supersecret"
Analytical Query
Retrieves the top 5 companies by total reported financial value from the data warehouse.

Bash

curl -X GET "http://localhost:8000/query/company-totals?limit=5" -u "admin:supersecret"
CRUD: Create a New Raw Submission
Creates a new raw submission record in the Bronze layer data.

Bash

curl -X 'POST' \
  'http://localhost:8000/raw/submissions/' \
  -u "admin:supersecret" \
  -H 'Content-Type: application/json' \
  -d '[
    {
      "adsh": "new-test-adsh-001",
      "cik": 1234567,
      "name": "New Test Corp",
      "form": "10-Q",
      "sic": 9999,
      "filing_summary": "This is a new filing created via the CRUD endpoint."
    }
  ]'
Optional Features Implemented
Software Engineering: Make a single docker compose with the necessary services - The entire application stack is orchestrated via a single docker-compose.yml file.