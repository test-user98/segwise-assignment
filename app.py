from fastapi import FastAPI, HTTPException, UploadFile, Form
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect, Table, Column, MetaData, text
from sqlalchemy.dialects.postgresql import VARCHAR, FLOAT, INTEGER
from sqlalchemy.exc import SQLAlchemyError
import aiohttp
import io
from sqlalchemy.dialects.postgresql import TIMESTAMP

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# Database Configuration
DB_URL = "postgresql+psycopg2://payjo:payjo@localhost:5432/csv_db"
engine = create_engine(DB_URL)
metadata = MetaData()

@app.post("/upload-csv/")
async def upload_csv(file_link: str = Form(...)):
    """
    Upload CSV from a public link and save its content to PostgreSQL.
    """
    try:
        # Download and process CSV
        async with aiohttp.ClientSession() as session:
            async with session.get(file_link) as response:
                if response.status != 200:
                    raise HTTPException(status_code=400, detail="Failed to download CSV file.")
                csv_data = await response.text()
        
        logger.debug("Reading CSV data")
        df = pd.read_csv(io.StringIO(csv_data))
        logger.debug(f"DataFrame shape: {df.shape}")
        
        # Clean column names - remove special characters and spaces
        df.columns = [col.strip().replace(' ', '_').replace('/', '_').replace('-', '_') 
                     for col in df.columns]
        
        # Handle missing column names
        if df.columns.hasnans:
            df.columns = [
                f"test_col{i}" if col.startswith("Unnamed") or pd.isna(col) else col
                for i, col in enumerate(df.columns)
            ]
        
        logger.debug(f"Columns after cleaning: {df.columns.tolist()}")
        
        # Process date columns
        for col in df.columns:
            if "date" in col.lower() or "release" in col.lower():
                try:
                    df[col] = df[col].apply(lambda x: handle_month_year_date(x))
                    df[col] = pd.to_datetime(df[col], errors='coerce', format="%b %d, %Y")
                except ValueError:
                    pass
        
        # Fill NaN values
        df = df.fillna(method='ffill')
        
        # Prepare table schema
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
        
        # Create table
        table_name = "csv_data"
        if inspect(engine).has_table(table_name):
            metadata.reflect(bind=engine)
            metadata.drop_all(bind=engine, tables=[metadata.tables[table_name]])
        
        table = Table(table_name, metadata, *table_columns, extend_existing=True)
        metadata.create_all(engine)
        
        # Convert DataFrame to list of dicts with proper handling of data types
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
            
        logger.debug(f"Sample record structure: {records[0] if records else 'No records'}")
        
        # Database insertion with explicit transaction
        with engine.begin() as connection:
            logger.debug("Starting database insertion")
            
            try:
                # Insert records one by one to better handle errors
                for idx, record in enumerate(records):
                    try:
                        connection.execute(table.insert().values(**record))
                        if idx % 100 == 0:  # Log progress every 100 records
                            logger.debug(f"Inserted {idx + 1} records")
                    except Exception as e:
                        logger.error(f"Error inserting record {idx}: {str(e)}")
                        logger.error(f"Problematic record: {record}")
                        raise
                
                # Verify record count
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                logger.debug(f"Verified {count} records in database")
                
            except Exception as e:
                logger.error(f"Error during insertion: {str(e)}")
                raise
        
        # Final verification
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            final_count = result.scalar()
            logger.debug(f"Final record count: {final_count}")
            
            if final_count == 0:
                raise Exception("No records were persisted in the database")
            
            # Get sample of inserted data
            sample_result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 1"))
            sample_data = sample_result.fetchone()
            
            return {
                "message": "CSV uploaded and saved successfully.",
                "records_processed": final_count,
                "sample_record": dict(sample_data._mapping) if sample_data else None
            }
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

def handle_month_year_date(date_value):
    if isinstance(date_value, str):
        parts = date_value.split()
        if len(parts) == 2:
            return f"{parts[0]} 1, {parts[1]}"
    return date_value
