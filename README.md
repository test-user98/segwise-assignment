Here’s the README file for your project:

---

# FastAPI CSV Upload and Data Explorer

This project is a **FastAPI-based microservice** that allows users to upload CSV files from a URL, save the data into a **PostgreSQL database**, and query the data using flexible filters through API endpoints.

## Features

### 1. **Authentication**
- API authentication using **Bearer Token**.
- Tokens are validated through the `Authorization` header.

### 2. **Upload CSV from URL**
- Download a CSV file from a given URL.
- Process and sanitize column names for compatibility.
- Automatically convert date columns to datetime format and handle missing data using **forward fill**.
- Creates unique tables for each CSV.
- Automatically indexes columns for faster queries:
  - **GIN Index** for string columns.
  - **BTREE Index** for numeric columns.
  - **Date Index** for date columns.

### 3. **Metadata Tracking**
- A `url_metadata` table maintains mappings between URLs and their corresponding database tables.
- Prevents duplicate uploads by checking if a URL is already processed.

### 4. **Data Filtering**
- Retrieve data based on filters applied to any column in the table.
- Supports:
  - **Substring search** for string fields.
  - **Exact match** for numeric and date fields.

## API Endpoints

### 1. **Upload CSV**
**POST** `/upload-csv/`

#### Request Body:
```json
{
  "url": "https://example.com/sample.csv"
}
```

#### Headers:
```bash
Authorization: Bearer <your_token>
```

#### Response:
- If the CSV is successfully processed:
```json
{
  "message": "CSV uploaded and saved successfully.",
  "table_name": "sample_csv",
  "records_processed": 1200
}
```
- If the URL already exists in the database:
```json
{
  "detail": "The data of the parsed csv is already present in DB."
}
```

#### Sample `curl`:
```bash
curl -X POST http://127.0.0.1:8000/upload-csv/ \
-H "Authorization: Bearer <your_token>" \
-H "Content-Type: application/json" \
-d '{"url": "https://example.com/sample.csv"}'
```

---

### 2. **Data Explorer**
**POST** `/data-explorer/`

#### Request Body:
```json
{
  "url": "https://example.com/sample.csv",
  "column_name_1": "value_1",
  "column_name_2": "value_2"
}
```

#### Headers:
```bash
Authorization: Bearer <your_token>
```

#### Response:
- If data matches the filters:
```json
{
  "data": [
    {"column_name_1": "value_1", "column_name_2": "value_2"}
  ]
}
```
- If no data matches the filters:
```json
{
  "data": []
}
```

#### Sample `curl`:
```bash
curl -X POST http://127.0.0.1:8000/data-explorer/ \
-H "Authorization: Bearer <your_token>" \
-H "Content-Type: application/json" \
-d '{"url": "https://example.com/sample.csv", "column_name_1": "value"}'
```

---

## Why PostgreSQL?

### Pros:
1. **Feature-rich**:
   - Advanced indexing options like GIN and BTREE, crucial for our indexing requirements.
   - Native support for JSON and advanced querying.
2. **Relational model**: PostgreSQL excels at structured, relational data storage.

### Why not ClickHouse?
ClickHouse is a powerful columnar database optimized for analytical workloads. While it offers speed for analytical queries, it lacks:
- **Flexibility**: For OLTP workloads (frequent inserts/updates), PostgreSQL is more suited.
- **Indexing versatility**: GIN and BTREE indexes in PostgreSQL are better suited for our use case.
- **Ease of use**: PostgreSQL is widely adopted and easier to manage for general-purpose applications.

---

## Estimated Cost for Running on AWS

### Assumptions:
1. **Database**: RDS PostgreSQL instance (db.t3.medium) with 20 GB storage.
2. **API Server**: EC2 instance (t3.medium) hosting the FastAPI app.
3. **Traffic**: Moderate (200-500 API requests/day).

### AWS Services and Costs:
| Service            | Resource          | Monthly Cost |
|--------------------|-------------------|--------------|
| **RDS PostgreSQL** | db.t3.medium (20 GB SSD storage) | $34.00       |
| **EC2 Instance**   | t3.medium (1 vCPU, 2 GB RAM)      | $27.50       |
| **Bandwidth**      | 50 GB/month                    | $5.00        |

**Total Cost**: ~$66.50 for 30 days.


### Additional Notes:
- Costs may vary depending on traffic, storage, and bandwidth usage.
- Using serverless solutions like AWS Lambda and Aurora Serverless could further optimize costs for sporadic traffic.

---

## How to Run Locally

1. **Clone the Repository**:
   ```bash
   git clone <repo_url>
   cd <repo_directory>
   ```

2. **Set up Environment Variables**:
   Create a `.env` file:
   ```bash
   DB_URL=postgresql+psycopg2://username:password@localhost:5432/db_name
   API_TOKEN=your_token
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Application**:
   ```bash
   uvicorn main:app --reload
   ```

5. **Access API**:
   - Open: `http://127.0.0.1:8000/docs` for API documentation.
