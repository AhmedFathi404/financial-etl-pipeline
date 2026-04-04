FROM apache/airflow:2.7.1
RUN pip install --no-cache-dir pandas numpy prophet psycopg2-binary