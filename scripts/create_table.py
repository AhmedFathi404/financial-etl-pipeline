import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# الاتصال بقاعدة البيانات
conn = psycopg2.connect(
    host="localhost",
    database="financial_DB",
    user="postgres",
    password="Caesar233020",
    port=5432
)
cursor = conn.cursor()

# ========== إنشاء الجدول ==========
print("Creating table...")

cursor.execute("""
DROP TABLE IF EXISTS raw_transactions;

CREATE TABLE raw_transactions (
    id SERIAL PRIMARY KEY,
    transaction_description TEXT,
    category VARCHAR(100),
    country VARCHAR(100),
    currency VARCHAR(3),
    amount DECIMAL(15,2),
    transaction_date TIMESTAMP
);
""")
conn.commit()
print("Table created successfully")

# ========== تحميل البيانات ==========
print("Loading CSV data...")

df = pd.read_csv(r'C:\Users\ahmed\financial_etl_project\data\financial_transactions.csv')
print(f"Loaded {len(df)} records from CSV")

# تحويل التاريخ
df['transaction_date'] = pd.to_datetime(df['transaction_date'])

# تحويل إلى list of tuples
data = df[['transaction_description', 'category', 'country', 'currency',
           'amount', 'transaction_date']].values.tolist()

# إدراج في جدول raw_transactions
print("Inserting into database...")
execute_values(
    cursor,
    """
    INSERT INTO raw_transactions 
    (transaction_description, category, country, currency, amount, transaction_date)
    VALUES %s
    """,
    data
)

conn.commit()
print(f"Inserted {len(data)} records successfully")

cursor.close()
conn.close()
print("Done!")