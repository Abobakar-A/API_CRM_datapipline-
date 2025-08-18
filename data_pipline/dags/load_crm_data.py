from __future__ import annotations
import pendulum
import json
import requests
import psycopg2
import smtplib
from email.mime.text import MIMEText
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# 1. دوال المهام
# ----------------------------------------
def extract_data_from_api(endpoint):
    """
    Extracts data from a given API endpoint.
    
    Args:
        endpoint (str): The name of the API endpoint to call (e.g., 'customers').
    
    Returns:
        dict: The JSON response from the API.
    """
    try:
        response = requests.get(f"http://192.168.0.143:8000/{endpoint}")
        response.raise_for_status() # Raises an exception for bad status codes (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ حدث خطأ في استخراج البيانات من API: {e}")
        raise # Re-raise the exception to fail the task

def load_data_to_postgres(**kwargs):
    """
    Loads data into a PostgreSQL table using Airflow Connections for security.
    """
    data = kwargs['data']
    table_name = kwargs['table_name']
    
    if not data:
        print(f"❌ لا توجد بيانات للتحميل في جدول {table_name}.")
        return

    # سحب معلومات الاتصال من Airflow
    pg_conn = BaseHook.get_connection('postgres_default')
    
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=pg_conn.schema,
            user=pg_conn.login,
            password=pg_conn.password,
            host=pg_conn.host
        )
        cursor = conn.cursor()
        
        # Insert data in a loop (can be optimized for large datasets)
        for item in data:
            columns = ', '.join(item.keys())
            values = ', '.join(['%s'] * len(item))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(insert_query, list(item.values()))

        conn.commit()
        print(f"✔️ اكتمل تحميل {len(data)} صفًا إلى جدول {table_name}.")

    except Exception as e:
        print(f"❌ حدث خطأ أثناء التحميل: {e}")
        if conn:
            conn.rollback()
        raise e

    finally:
        if conn:
            cursor.close()
            conn.close()

# -------------------------------------------------------------------------------------------------
# 2. دوال الإشعار المخصصة (كمهام)
# ----------------------------------------
def send_success_email(**kwargs):
    """Sends a success email using smtplib."""
    try:
        api_key = Variable.get("sendgrid_api_key")
    except KeyError:
        print("❌ Error: Airflow Variable 'sendgrid_api_key' not found.")
        return

    subject = f"✅ Airflow DAG Success: {kwargs['dag_run'].dag_id}"
    html_content = f"The DAG '{kwargs['dag_run'].dag_id}' completed successfully."
    
    sender_email = "kokoslm1400@gmail.com"
    recipient_email = "kokoslm1400@gmail.com"

    msg = MIMEText(html_content, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email

    try:
        with smtplib.SMTP_SSL('smtp.sendgrid.net', 465) as server:
            server.login('apikey', api_key)
            server.send_message(msg)
        print("✔️ Success email alert sent.")
    except Exception as e:
        print(f"❌ Failed to send success email alert: {e}")
        raise

def send_failure_email(**kwargs):
    """Sends a failure email using smtplib."""
    try:
        api_key = Variable.get("sendgrid_api_key")
    except KeyError:
        print("❌ Error: Airflow Variable 'sendgrid_api_key' not found.")
        return

    subject = f"❌ Airflow DAG Failure: {kwargs['dag_run'].dag_id}"
    html_content = f"The DAG '{kwargs['dag_run'].dag_id}' has failed. Please check the logs."
    
    sender_email = "kokoslm1400@gmail.com"
    recipient_email = "kokoslm1400@gmail.com"

    msg = MIMEText(html_content, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email

    try:
        with smtplib.SMTP_SSL('smtp.sendgrid.net', 465) as server:
            server.login('apikey', api_key)
            server.send_message(msg)
        print("✔️ Failure email alert sent.")
    except Exception as e:
        print(f"❌ Failed to send failure email alert: {e}")
        raise
# -------------------------------------------------------------------------------------------------

# 3. إعداد الـ DAG
# ----------------------------------------
with DAG(
    dag_id="full_pipeline_v6",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["crm", "postgres"],
) as dag:
    
    # 4. تعريف المهام (Tasks)
    # ----------------------------------------
    create_customers_table = SQLExecuteQueryOperator(
        task_id="create_customers_table",
        conn_id="postgres_default",
        sql="""
            DROP TABLE IF EXISTS customers CASCADE;
            CREATE TABLE customers (
                customer_id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                region VARCHAR(255)
            );
        """
    )
    
    create_purchases_table = SQLExecuteQueryOperator(
        task_id="create_purchases_table",
        conn_id="postgres_default",
        sql="""
            DROP TABLE IF EXISTS purchases CASCADE;
            CREATE TABLE purchases (
                purchase_id INT PRIMARY KEY,
                customer_id INT,
                amount DECIMAL(10, 2),
                date DATE,
                product_id INT 
            );
        """
    )
    
    create_products_table = SQLExecuteQueryOperator(
        task_id="create_products_table",
        conn_id="postgres_default",
        sql="""
            DROP TABLE IF EXISTS products CASCADE;
            CREATE TABLE products (
                product_id INT PRIMARY KEY,
                product_name VARCHAR(255),
                category VARCHAR(255),
                price DECIMAL(10, 2)
            );
        """
    )

    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_data_from_api,
        op_kwargs={"endpoint": "customers"}
    )

    extract_purchases = PythonOperator(
        task_id="extract_purchases",
        python_callable=extract_data_from_api,
        op_kwargs={"endpoint": "purchases"}
    )
    
    extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=extract_data_from_api,
        op_kwargs={"endpoint": "products"}
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_data_to_postgres,
        op_kwargs={
            "data": extract_customers.output,
            "table_name": "customers"
        }
    )

    load_purchases = PythonOperator(
        task_id="load_purchases",
        python_callable=load_data_to_postgres,
        op_kwargs={
            "data": extract_purchases.output,
            "table_name": "purchases"
        }
    )
    
    load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_data_to_postgres,
        op_kwargs={
            "data": extract_products.output,
            "table_name": "products"
        }
    )
    
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="dbt run --project-dir /usr/local/airflow/dags/dbt_crm --profiles-dir /usr/local/airflow/dags/dbt_crm",
    )
    
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="dbt test --project-dir /usr/local/airflow/dags/dbt_crm --profiles-dir /usr/local/airflow/dags/dbt_crm",
    )

    email_success_task = PythonOperator(
        task_id="email_success_task",
        python_callable=send_success_email,
        trigger_rule="all_success",
    )
    
    email_failure_task = PythonOperator(
        task_id="email_failure_task",
        python_callable=send_failure_email,
        trigger_rule="one_failed",
    )

    # 5. تحديد الترتيب (Flow)
    # ----------------------------------------
    create_customers_table >> extract_customers >> load_customers
    create_purchases_table >> extract_purchases >> load_purchases
    create_products_table >> extract_products >> load_products

    [load_customers, load_purchases, load_products] >> run_dbt_models >> run_dbt_tests

    [run_dbt_tests] >> email_success_task
    [run_dbt_tests] >> email_failure_task