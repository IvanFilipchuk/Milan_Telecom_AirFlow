# **Telecom Data Pipeline Project**

This project demonstrates a data pipeline designed to process and analyze telecom data using Apache Airflow, Apache Spark, and Docker. The pipeline includes multiple layers of data transformation (bronze, silver, and gold) and supports scalable processing through Spark.

---

## **Features**

- **Bronze Layer**: Raw data ingestion from CSV files.
- **Silver Layer**: Data cleaning and normalization for specific use cases (internet, SMS & calls, user events).
- **Gold Layer**: Aggregated analytics based on `GridID` and `CountryCode`.
- **Containerized Execution**: Fully containerized using Docker Compose.
- **Workflow Management**: Apache Airflow DAG for task orchestration.
- **Scalable Processing**: Apache Spark for distributed data processing.

---

## **Pipeline Overview**

### **1. Bronze Layer**
- Ingests raw telecom data from a CSV file.
- Stores raw data in a structured directory (`/opt/data/bronze`).

### **2. Silver Layer**
- Cleans and transforms data into three normalized datasets:
  - **Internet Usage**: Extracts and filters internet traffic data.
  - **SMS and Call Data**: Computes SMS and call-related metrics.
  - **User Events**: Categorizes traffic type and calculates traffic amounts.
- Outputs data to `/opt/data/silver`.

### **3. Gold Layer**
- Performs aggregations for analytical insights:
  - **By GridID**: Aggregates data based on grid identifiers.
  - **By CountryCode**: Aggregates data based on country codes.
- Outputs data to `/opt/data/gold`.

---

## **DAG Workflow**

The data pipeline workflow is managed using Apache Airflow. Below is the Airflow DAG graph visualizing the pipeline:

![Airflow DAG Graph](![image](https://github.com/user-attachments/assets/ae1f0b0e-3249-4ed1-8368-e3ad9fd28bbb))

1. **Start**: Initializes the pipeline.
2. **Bronze Layer**:
    - Ingests raw data from the CSV file.
3. **Silver Layer**:
    - Normalizes data for:
      - Internet Usage.
      - SMS and Call Data.
      - User Events.
4. **Gold Layer**:
    - Aggregates data:
      - By `GridID`.
      - By `CountryCode`.
5. **End**: Marks the pipeline as completed.

---
```bash
docker-compose up -d --build
```
