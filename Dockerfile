FROM python:3.12.7
RUN pip install pandas sqlalchemy psycopg2 requests

WORKDIR /app
COPY ingest_data.py  ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]