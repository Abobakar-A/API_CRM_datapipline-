# CRM Data Pipeline Project

This is an end-to-end data pipeline project designed to extract customer and purchase data from an external API, transform it, and load it into a PostgreSQL database for analysis. The project uses open-source tools to manage and automate each step of the process.

---

## Technologies Used

* **Astro / Apache Airflow:** For orchestrating and automating the Directed Acyclic Graph (DAG) of the data pipeline.
* **Python:** For writing custom functions to extract and load data from the API.
* **dbt (Data Build Tool):** To transform raw data within the database into clean, optimized analytical models.
* **PostgreSQL:** As the target database to store both the raw and transformed data.
* **Git:** For version control and tracking changes to the project code.

---

## Project Workflow and Steps

The pipeline was built in logical stages to ensure data flows correctly from the source to the final destination.

1.  **Extract & Load:**
    * The DAG in Airflow initiates the process.
    * It uses custom Python functions to pull customer and purchase data from an external API.
    * The raw data is loaded directly into tables within the **`public`** schema of the PostgreSQL database.

2.  **Data Transformation:**
    * After the raw data is loaded, a separate `dbt` task is triggered by Airflow.
    * `dbt` uses predefined models to transform the raw data into useful analytical models.
    * The transformed models (e.g., `stg_customers` and `fact_customer_purchases`) are saved in a separate **`dbt_schema`** to ensure data organization.

3.  **Data Quality:**
    * A `dbt test` task runs after the transformation step to ensure the integrity and quality of the data.
    * These tests validate basic rules such as the absence of null values and primary key uniqueness.

4.  **Orchestration:**
    * Apache Airflow acts as the orchestrator, ensuring that each task (extract, load, transform, test) is executed in the correct sequence.
    * In case of success or failure, email notifications are sent to report the status of the workflow.

---

## How to Run the Project

To run this project, follow these steps from the root directory of your project in the terminal:

1.  **Start the Environment:**
    ```bash
    astro dev start
    ```
2.  **Restart the Environment:**
    ```bash
    astro dev restart
    ```
3.  **Stop the Environment:**
    ```bash
    astro dev stop
    ```
4.  **Force-stop all containers:**
    ```bash
    astro dev kill
    ```
