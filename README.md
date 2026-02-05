## 🔗 VectraSQL
VectraSQL is an intelligent invoice processing system that leverages a dual-database architecture to handle both unstructured and structured data efficiently. It combines MongoDB Atlas for vector-based semantic search (using embeddings for natural language queries) and MySQL for structured, relational data storage. The system uses Large Language Models (LLMs) for OCR text extraction from images, data structuring, and semantic retrieval, orchestrated via Apache Airflow for automated data synchronization.
This project uses:
- MongoDB Atlas for vector storage and semantic search
- MySQL for structured data warehousing
- Hugging Face embeddings (BAAI/bge-large-en-v1.5)
- Groq-hosted LLMs (LLaMA models) for OCR and generation
- Streamlit for the UI
- Apache Airflow for automated data synchronization
- Docker Compose for containerized deployment
___
### 🏗️ Project Structure
```text
vectraSQL/
│
├── dags/
│   └── atlas_to_mysql_sync.py  # Airflow DAG for MongoDB to MySQL sync
├── streamlit_app/
│   ├── app.py                  # Streamlit invoice processor UI
│   ├── dbclient.py             # MongoDB client utilities
│   ├── embeddings.py           # Hugging Face embedding generation
│   ├── extractor.py            # LLM-based text extraction from images
│   └── retriever.py            # Semantic search and answer generation
├── logs/                       # Airflow logs (generated)
├── myenv/                      # Virtual environment (optional)
├── docker-compose.yml          # Services for Airflow, MySQL, PostgreSQL
├── init.sql                    # MySQL table initialization
├── requirements.txt            # Python dependencies
├── .env                        # API keys and URIs (not committed)
```
___
### 🧠 Architecture Overview
```mermaid
graph TD;
    User([User]) --> UI[Streamlit UI];
    UI -->|Invoice Images| Extract[LLM OCR & Structuring];
    Extract -->|Raw Text + JSON| Embed[Hugging Face Embeddings];
    Embed -->|Vectors + Metadata| Mongo[(MongoDB Atlas)];
    Mongo -->|Unsynced Docs| Airflow[Airflow DAG];
    Airflow -->|Structured Data| MySQL[(MySQL)];
    UI -->|Query| Retrieve[Vector Search];
    Retrieve -->|Context| LLM[Groq LLaMA 3.3 70B];
    LLM -->|Answer| UI;
```
___
### ⚙️ Prerequisites
Make sure you have the following installed:
- Python 3.8+
- Docker & Docker Compose
- MongoDB Atlas account (with vector index configured)
- Hugging Face API key
- Groq API key
___
### 🔐 Environment Variables
Create a .env file in the project root:
1. MongoDB Atlas
- MONGO_URI = "mongodb+srv://<username>:<password>@<clusterid>.mongodb.net/?appName=<clustername>"

2. AI API Keys
- GROQ_API_KEY="your_groq_key_here"
- HF_API_KEY="your_huggingface_key_here"

3. MySQL Config
- MYSQL_ROOT_PASSWORD="set_your_root_password"
- MYSQL_DATABASE="set_your_db_name"
___
### 📦 Installation
1️. Clone the Repository
- git clone <repository-url>
- cd vectraSQL

2️. Install Python Dependencies
- pip install -r requirements.txt

3️. Start Databases and Airflow
- docker-compose up --build -d
- This starts MySQL (port 3307), PostgreSQL, and Airflow (webserver at http://localhost:8080, login: admin/admin).
- Data persists in Docker volumes.

4️. Configure Airflow Connections and Variables

**Step 1: Create/Edit the MySQL Connection**
By default, the mysql_default connection ID is created when you run docker-compose up, based on the environment variable in the YAML. However, you may need to verify or edit its details.
Open localhost:8080 and log in.

- Go to Admin > Connections.
- If mysql_default exists, click on it to edit. If not, click the + (plus) sign to add a new connection with ID mysql_default.
- Fill in or ensure these exact details:
- Connection Id: mysql_default (The DAG looks for this name).
- Connection Type: MySQL.
- Host: mysql (This is the service name from your YAML).
- Schema:  [value of MYSQL_DATABASE from .env]
- Login: root.
- Password: [value of MYSQL_ROOT_PASSWORD from .env]
- Port: 3306 (Internal Docker port).
- Click Save.

**Step 2: Trigger the Sync**
- Go back to the DAGs home page.
- Toggle the switch next to atlas_to_mysql_sync to ON (Blue).
- On the far right, click the Play button (Trigger DAG).

**Step 3: Set up New Connection in MySQL Workbench**
To connect externally to MySQL (e.g., for querying or debugging), use MySQL Workbench or a client.

Create a new connection with:

- Host: localhost
- Port: 3307 (External port from docker-compose).
- Username: root
- Password: [value of MYSQL_DATABASE from .env]
- Database: [value of MYSQL_ROOT_PASSWORD from .env]

5️. Run the Streamlit App
- streamlit run streamlit_app/app.py
- The app will be available at: http://localhost:8501
___
### 🚀 How to Use
Step 1: Configure MongoDB
- In the Streamlit sidebar, enter Atlas credentials (username, password, cluster name, ID, DB name, collection, vector index).

Step 2: Upload Invoices
- Upload invoice images (PNG/JPG).
- Enter a title (optional).
- Click "Ingest Images to Database" to extract, embed, and store in MongoDB.

Step 3: Query and Sync
- Ask semantic questions (e.g., "Total for vendor X?").
- Airflow auto-syncs to MySQL every 5 minutes.
- Check Airflow UI for sync status.
___
### 🧩 Future Improvements
- Hierarchical Extraction: Implement multi-level parsing for complex invoices (e.g., line items).
- Prompt Enhancements: Improve LLM prompts for better value parsing.
- Dropdown for existing collections/indexes
- Delete/reset sync support
- Chunk & embedding statistics
- Metadata-based filtering in searches
- Streaming LLM responses for queries
___
📜 License
This project is licensed under the MIT License.
