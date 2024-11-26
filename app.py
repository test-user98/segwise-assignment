from fastapi import FastAPI, HTTPException, Header, Depends
import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, MetaData, text, select, DateTime, Integer, Float, String
from sqlalchemy.dialects.postgresql import VARCHAR, FLOAT, INTEGER, TIMESTAMP
import aiohttp
from pydantic_settings import BaseSettings
import io
from pydantic import BaseModel
from typing import Optional
import os
from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import VARCHAR, FLOAT, INTEGER, TIMESTAMP
from urllib.parse import urlparse

class Settings(BaseSettings):
    DB_URL: str
    API_TOKEN: str

    class Config:
        env_file = ".env"

settings = Settings()

app = FastAPI()

# Database Configuration
engine = create_engine(settings.DB_URL)
metadata = MetaData()

# Create a metadata table to track URLs and their associated tables
url_metadata_table = Table(
    "url_metadata", metadata,
    Column("url", VARCHAR, primary_key=True),
    Column("table_name", VARCHAR, unique=True)
)
metadata.create_all(engine)

def authenticate_user(authorization: Optional[str] = Header(None)):
    if authorization != f"Bearer {settings.API_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

class FileLinkRequest(BaseModel):
    url: str


def create_indexes_for_table(table, df, dtype_map):
    # Create GIN indexes for string columns
    for col, dtype in df.dtypes.items():
        if dtype == "object":  # For string columns
            index_name = f"{table.name}_{col}_gin_index"
            # Create GIN index using raw SQL for text columns
            with engine.connect() as conn:
                conn.execute(
                    text(f'CREATE INDEX IF NOT EXISTS {index_name} ON {table.name} USING gin ({col} gin_trgm_ops)')
                )
    
        if dtype == "datetime64[ns]":  # For date columns
            index_name = f"{table.name}_{col}_date_index"
            index = Index(index_name, Column(str(col), TIMESTAMP))
            index.create(bind=engine)

        if dtype in ["int64", "float64"]:  # For numeric columns
            index_name = f"{table.name}_{col}_btree_index"
            index = Index(index_name, Column(str(col), dtype_map.get(str(dtype), VARCHAR)))
            index.create(bind=engine)

def generate_table_name(url: str, existing_tables: set) -> str:
    # Extract file name from URL
    parsed_url = urlparse(url)
    file_name = os.path.basename(parsed_url.path).split("?")[0]  # Remove query params if any
    table_name_base = file_name.replace(".", "_") if file_name else "csv_data"

    # Ensure uniqueness
    table_name = table_name_base
    counter = 1
    while table_name in existing_tables:
        table_name = f"{table_name_base}_{counter}"
        counter += 1

    return table_name

def handle_month_year_date(date_value):
    if isinstance(date_value, str):
        parts = date_value.split()
        if len(parts) == 2:
            return f"{parts[0]} 1, {parts[1]}"
    return date_value

@app.post("/upload-csv/")
async def upload_csv(
    request: FileLinkRequest,
    authorization: str = Depends(authenticate_user)
):
    try:
        # Check if URL already exists in metadata table
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT table_name FROM url_metadata WHERE url = :url"), {"url": request.url}
            )
            existing_entry = result.fetchone()
            if existing_entry:
                raise HTTPException(
                    status_code=200,
                    detail=f"The data of the parsed csv is already present in DB'."
                )

        # Download CSV
        async with aiohttp.ClientSession() as session:
            async with session.get(request.url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=400, detail="Failed to download CSV file.")
                csv_data = await response.text()

        # Parse CSV
        df = pd.read_csv(io.StringIO(csv_data))
        df.columns = [col.strip().lower().replace(' ', '_').replace('/', '_').replace('-', '_') for col in df.columns]

        if df.columns.hasnans:
            df.columns = [
                f"test_col{i}" if col.startswith("Unnamed") or pd.isna(col) else col
                for i, col in enumerate(df.columns)
            ]

        for col in df.columns:
            if "date" in col.lower() or "release" in col.lower():
                try:
                    df[col] = df[col].apply(lambda x: handle_month_year_date(x))
                    df[col] = pd.to_datetime(df[col], errors='coerce', format="%b %d, %Y")
                except ValueError:
                    pass

        df = df.fillna(method='ffill')

        dtype_map = {
            "object": VARCHAR,
            "float64": FLOAT,
            "int64": INTEGER,
            "datetime64[ns]": TIMESTAMP,
        }

        table_columns = [
            Column(str(col), dtype_map.get(str(dtype), VARCHAR)) 
            for col, dtype in df.dtypes.items()
        ]

        # Generate unique table name
        existing_tables = set(inspect(engine).get_table_names())
        table_name = generate_table_name(request.url, existing_tables)

        # Create table
        table = Table(table_name, metadata, *table_columns, extend_existing=True)
        metadata.create_all(engine)

        # Create indexes for columns in the created table
        create_indexes_for_table(table, df, dtype_map)

        records = []
        for _, row in df.iterrows():
            record = {}
            for column in df.columns:
                value = row[column]
                if pd.isna(value):
                    record[str(column)] = None
                elif isinstance(value, pd.Timestamp):
                    record[str(column)] = value.to_pydatetime()
                else:
                    record[str(column)] = value
            records.append(record)

        with engine.begin() as connection:
            try:
                for record in records:
                    connection.execute(table.insert().values(**record))
                
                # Insert URL and table name into metadata table
                connection.execute(
                    url_metadata_table.insert().values(url=request.url, table_name=table_name)
                )

                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Insertion error: {str(e)}")

        return {
            "message": "CSV uploaded and saved successfully.",
            "table_name": table_name,
            "records_processed": count
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.post("/data-explorer/")
async def data_explorer(
    filters: dict,  
    authorization: str = Depends(authenticate_user)
):

    url = filters.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="URL is required")
    
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT table_name FROM url_metadata WHERE url = :url"), {"url": url}
        )
        row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="URL not found in the database")


    table = Table(row[0], metadata, autoload_with=engine)

    query = select(table)


    for field, value in filters.items():
        if field != "url":
            field = field.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            if field in table.columns:
                column = table.columns[field]
                # Handle numerical, string, and date types
                if isinstance(column.type, (Integer, Float)):
                    # For numerical fields, exact match
                    query = query.where(column == value)
                elif isinstance(column.type, String):
                    # For string fields, substring match
                    query = query.where(column.ilike(f"%{value}%"))
                elif isinstance(column.type, DateTime):
                    # For date fields, exact match
                    query = query.where(column == value)
                else:
                    raise HTTPException(status_code=400, detail="Unsupported field type")
            else:
                raise HTTPException(status_code=400, detail=f"Field '{field}' not found in table")

    # Execute the query and return the results
    with engine.connect() as conn:
        result = conn.execute(query)
        records = result.fetchall()

        if records:
            return {"data": [row._asdict() for row in records]}
        else:
            return {"data": []}