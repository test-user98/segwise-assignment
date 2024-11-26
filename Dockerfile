# Base image
FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc

COPY requirements.txt .
COPY .env .env
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .


EXPOSE 8000

# Run the FastAPI application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
