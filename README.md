# 🧠 API CRM Data Pipeline

## 📌 Overview
This project simulates a production-grade CRM data pipeline using Python, FastAPI, PostgreSQL, and dbt. It generates realistic customer and purchase data, exposes it via a RESTful API, and transforms it using dbt models. The pipeline is modular, scalable, and designed to reflect real-world business logic.

## 🛠️ Tech Stack
- **FastAPI**: RESTful API for serving customer and purchase data
- **PostgreSQL**: Relational database for persistent storage
- **Python + Faker**: Synthetic data generation with realistic patterns
- **dbt**: Data transformation, testing, and and documentation
- **Airflow**: Workflow orchestration for full automation

---

## 📁 Project Structure
API_CRM_datapipline-/
├── crm_api/                # FastAPI application
├── data_pipeline/          # Data generation and ingestion scripts
├── dbt_crm/                # dbt models and configurations
├── logs/                   # Runtime logs and error tracking
└── README.md               # Project documentation


---

## 🚀 How to Run

### 1. Clone the repository
```bash
git clone [https://github.com/Abobakar-A/API_CRM_datapipline-.git](https://github.com/Abobakar-A/API_CRM_datapipline-.git)
cd API_CRM_datapipline-
2. Setup the environment
Bash

python -m venv venv
source venv/bin/activate       # or venv\Scripts\activate on Windows
pip install -r requirements.txt
3. Generate data and run the pipeline
Run the Airflow DAG full_pipeline_v6 to execute the data pipeline. This will:

Generate and extract new data.

Load data into PostgreSQL tables.

Run dbt models and tests to transform the data.

4. Access API docs
Visit http://localhost:8000/docs for the interactive Swagger UI.

✅ Features Implemented
Integrated Airflow: The entire pipeline, from data generation to dbt transformation, is now orchestrated by a single Airflow DAG.

Customer Segmentation: Implemented get_customer_segment as a dbt macro, allowing for flexible and reusable customer classification logic.

Advanced Analytical Models: Created new dbt models to derive key business insights:

fact_customer_lifetime_value: Calculates customer-level metrics like total spending and purchase frequency.

fact_product_performance: Analyzes sales performance for each product.

Robust Data Quality: All data pipeline issues, from data ingestion to dbt transformations, have been successfully resolved, ensuring a stable and reliable pipeline.

📊 dbt Models
stg_* Models: Staging models that perform light cleaning and prepare raw data for transformation.

fact_customer_lifetime_value: Calculates customer lifetime value and key engagement metrics.

fact_product_performance: Aggregates product sales data to analyze performance.

fact_customer_purchases: Links all relevant information for each purchase.

fact_regional_sales: Provides aggregated sales data by region.

dim_customers: A dimension table for detailed customer information and segmentation.

📌 Future Enhancements
Automated Data Tests: Add custom dbt tests to ensure data integrity beyond standard checks.

Dockerization: Add Docker support for easy deployment and environment management.

API Enhancements: Extend the FastAPI with advanced features like filtering, pagination, and a simple authentication layer.

👤 Author
Abobakr - Focused data engineer passionate about building realistic, production-grade pipelines using FastAPI, dbt, and PostgreSQL.
