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

# 1. دوال المهام
# ----------------------------------------
def extract_customers_from_api():
    response = requests.get("http://192.168.0.143:8000/customers")
    response.raise_for_status()
    return response.json()

def extract_purchases_from_api():
    response = requests.get("http://192.168.0.143:8000/purchases")
    response.raise_for_status()
    return response.json()

def load_data_to_postgres(**kwargs):
    data = kwargs['data']
    table_name = kwargs['table_name']
    
    if not data:
        print(f"❌ لا توجد بيانات للتحميل في جدول {table_name}.")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="postgres"
        )
        cursor = conn.cursor()
        
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
    """
    Sends a success email using smtplib.
    """
    try:
        api_key = Variable.get("sendgrid_api_key")
    except KeyError:
        print("❌ Error: Airflow Variable 'sendgrid_api_key' not found.")
        return

    subject = f"✅ Airflow DAG Success: {kwargs['dag'].dag_id}"
    html_content = f"The DAG '{kwargs['dag'].dag_id}' completed successfully."
    
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
        raise # Re-raise to mark the task as failed

def send_failure_email(**kwargs):
    """
    Sends a failure email using smtplib.
    """
    try:
        api_key = Variable.get("sendgrid_api_key")
    except KeyError:
        print("❌ Error: Airflow Variable 'sendgrid_api_key' not found.")
        return

    subject = f"❌ Airflow DAG Failure: {kwargs['dag'].dag_id}"
    html_content = f"The DAG '{kwargs['dag'].dag_id}' has failed. Please check the logs."
    
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
        raise # Re-raise to mark the task as failed

# -------------------------------------------------------------------------------------------------

# 3. إعداد الـ DAG
# ----------------------------------------
with DAG(
    dag_id="full_pipeline_v5",
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
                date DATE
            );
        """
    )
    
    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers_from_api,
    )

    extract_purchases = PythonOperator(
        task_id="extract_purchases",
        python_callable=extract_purchases_from_api,
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

    # مهام الإشعارات الجديدة
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

    # إضافة مهام الإشعارات إلى نهاية الـ DAG
    [load_customers, load_purchases] >> email_success_task
    [load_customers, load_purchases] >> email_failure_task