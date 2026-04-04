from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging


default_args = {
    'owner': 'ahmed',
    'depends_on_past': True,
    'start_date': datetime(2025, 1, 1),
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_daily_pipeline',
    default_args=default_args,
    description='ETL Daily Pipeline For Financial Data',
    schedule_interval='0 0 * * *',
    catchup=True,
    max_active_runs=1,
    tags=['financial','etl'],
)

def extract_data(**kwargs):
    execution_date = kwargs['ds']
    print(f"data for date: {execution_date}")

    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    cursor.execute("SELECT current_database();")
    db_name = cursor.fetchone()
    print(f"Connected to database: {db_name[0]}")
    cursor.execute("""
        SELECT transaction_description, category, country, currency, amount, transaction_date
        FROM public.raw_transactions
        WHERE transaction_date::date = %s
    """, (execution_date,))
    print(f"Connected to database: {conn.dsn}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    columns = ['transaction_description', 'category', 'country', 'currency', 'amount', 'transaction_date']
    filtered_dict = [dict(zip(columns, row)) for row in rows]

    print(f"Records for {execution_date}: {len(filtered_dict)}")

    kwargs['ti'].xcom_push(key='extracted_data', value=filtered_dict)


def clean_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(key='extracted_data')

    if not extracted_data:
        print("No data received from extract_data")
        return

    df = pd.DataFrame(extracted_data)

    exchange_rate = {
        'USD': 1.0,
        'AUD': 0.65,
        'GBP': 1.25,
        'CAD': 0.73,
        'INR': 0.012
    }

    income_categories =['Income']

    def convert_income_to_usd(raw):
        rate = exchange_rate.get(raw['currency'], 1.0)
        return float(raw['amount']) * rate

    df['amount_usd'] = df.apply(convert_income_to_usd, axis=1)
    df['is_income'] = df['category'].apply(lambda x: x in income_categories)

    initial_count = len(df)
    df = df.dropna(subset=['transaction_description','amount','category','country','currency','transaction_date'])
    final_count = len(df)

    if initial_count != final_count:
        print(f"Dropped {initial_count - final_count} records with null values")

    df['transaction_date'] = df['transaction_date'].astype(str)
    cleaned_dict = df.to_dict('records')
    ti.xcom_push(key='cleaned_data', value=cleaned_dict)
    print(f"Cleaned data ready: {len(cleaned_dict)} records")


def calculate_daily_aggregation(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(key='cleaned_data')

    if not cleaned_data:
        print("No data received from calculate_daily_aggregation")
        return

    df = pd.DataFrame(cleaned_data)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    total_income = df[df['is_income'] == True]['amount_usd'].sum()
    total_expense = df[df['is_income'] == False]['amount_usd'].sum()
    net_cashflow = total_income - total_expense
    transaction_count = len(df)

    transaction_date = df['transaction_date'].iloc[0].date()

    daily_aggregation = {
        'transaction_date': str(transaction_date),
        'total_income': float(total_income),
        'total_expense': float(total_expense),
        'net_cashflow': float(net_cashflow),
        'transaction_count': transaction_count,
    }

    ti.xcom_push(key='daily_aggregation', value=daily_aggregation)


def update_running_balance(**kwargs):
    ti = kwargs['ti']
    daily_aggregation = ti.xcom_pull(key='daily_aggregation')

    if not daily_aggregation:
        return

    transaction_date = daily_aggregation['transaction_date']
    net_cashflow = daily_aggregation['net_cashflow']

    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT end_of_day_balance
        FROM running_balance
        ORDER BY balance_date DESC
        LIMIT 1
    """)

    last_balance_row = cursor.fetchone()  
    
    if last_balance_row and last_balance_row[0] is not None:
        last_balance = float(last_balance_row[0])
    else:
        last_balance = 10000.0

    new_balance = net_cashflow + last_balance

    cursor.execute("""
        INSERT INTO running_balance (balance_date, end_of_day_balance, created_at)
        VALUES (%s, %s, NOW())
    """, (transaction_date, new_balance))

    conn.commit()
    cursor.close()
    conn.close()

    ti.xcom_push(key='balance_details', value={
        'net_cashflow': net_cashflow,
        'transaction_date': transaction_date,
        'end_of_day_balance': new_balance,
    })
def data_quality_check(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(key='cleaned_data')

    if not cleaned_data:
        return

    df = pd.DataFrame(cleaned_data)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    expected_currencies = ['USD', 'AUD', 'GBP', 'CAD', 'INR']
    total_null = int(df.isnull().sum().sum())
    num_of_unknown_currency = int(df[df['currency'].isin(expected_currencies)].shape[0])
    negative_amount = int (df[df['amount'] < 0].shape[0])

    total_income =float( df[df['is_income'] == True]['amount_usd'].sum())
    total_expense =float( df[df['is_income'] == False]['amount_usd'].sum())

    income_expense_ratio =float( total_income / total_expense if total_expense > 0 else 0)
    check_date = df['transaction_date'].iloc[0].date()

    pgs_hook = PostgresHook(postgres_conn_id='postgres')
    connection = pgs_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("""
        INSERT INTO data_quality_metrics 
        (check_date, total_records, null_count, unknown_currency_count, 
         negative_amount_count, income_expense_ratio, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """, (check_date, len(df), total_null, num_of_unknown_currency,
          negative_amount, income_expense_ratio))

    connection.commit()
    cursor.close()
    connection.close()


def load_data(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(key='cleaned_data')
    daily_agg = ti.xcom_pull(key='daily_aggregation')

    if not cleaned_data:
        print("No cleaned data to load")
        return

    print(f"Loading {len(cleaned_data)} transactions and daily aggregates")

    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # تخزين الإجماليات اليومية
    if daily_agg:
        cursor.execute("""
            INSERT INTO daily_aggregates 
            (transaction_date, total_income, total_expense, net_cashflow, transaction_count, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (transaction_date) DO UPDATE SET
                total_income = EXCLUDED.total_income,
                total_expense = EXCLUDED.total_expense,
                net_cashflow = EXCLUDED.net_cashflow,
                transaction_count = EXCLUDED.transaction_count,
                created_at = NOW()
        """, (
            daily_agg['transaction_date'],
            daily_agg['total_income'],
            daily_agg['total_expense'],
            daily_agg['net_cashflow'],
            daily_agg['transaction_count']
        ))

        print(f"Inserted/Updated daily aggregates for {daily_agg['transaction_date']}")

    # تسجيل نجاح الـ ETL
    cursor.execute("""
        INSERT INTO etl_logs 
        (task_name, status, records_processed, error_message, execution_time_seconds, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (
        'load_data',
        'SUCCESS',
        len(cleaned_data),
        None,
        0
    ))

    conn.commit()
    print("ETL log recorded")

    cursor.close()
    conn.close()
    print("All data loaded successfully")


# tasks
extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
clean_data_task = PythonOperator(task_id='clean_data', python_callable=clean_data, dag=dag)
calculate_daily_aggregation_task = PythonOperator(task_id='calculate_daily_aggregation', python_callable=calculate_daily_aggregation, dag=dag)
update_running_balance_task = PythonOperator(task_id='update_running_balance', python_callable=update_running_balance, dag=dag)
data_quality_check_task = PythonOperator(task_id='data_quality_check', python_callable=data_quality_check, dag=dag)
load_data_task = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)

extract_task >> clean_data_task >> calculate_daily_aggregation_task >> update_running_balance_task >> data_quality_check_task >> load_data_task