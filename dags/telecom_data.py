import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="telecom_data",
    default_args={
        "owner": "Ivan Filipchuk",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Pipeline started"),
    dag=dag
)

bronze_layer = SparkSubmitOperator(
    task_id="load_to_bronze_layer",
    conn_id="spark-conn",
    application="jobs/python/bronze_layer/load_data_to_bronze.py",
    application_args=["/opt/data/data1.csv", "/opt/data/bronze/sample_data", "2"],
    dag=dag
)

silver_internet = SparkSubmitOperator(
    task_id="silver_normalization_internet",
    conn_id="spark-conn",
    application="jobs/python/silver_layer/normalize_internet.py",
    application_args=["/opt/data/bronze/sample_data", "/opt/data/silver/internet", "2"],
    dag=dag
)

silver_sms_call = SparkSubmitOperator(
    task_id="silver_normalization_sms_call",
    conn_id="spark-conn",
    application="jobs/python/silver_layer/normalize_sms_call.py",
    application_args=["/opt/data/bronze/sample_data", "/opt/data/silver/sms_call", "2"],
    dag=dag
)

silver_user_events = SparkSubmitOperator(
    task_id="silver_normalization_user_events",
    conn_id="spark-conn",
    application="jobs/python/silver_layer/normalize_user_events.py",
    application_args=["/opt/data/bronze/sample_data", "/opt/data/silver/user_events", "2"],
    dag=dag
)

golden_aggregation_by_gridid = SparkSubmitOperator(
    task_id="golden_aggregation_by_gridid",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/aggregate_by_gridid.py",
    application_args=["/opt/data/silver", "/opt/data/gold/aggregated_by_gridid", "1"],
    dag=dag
)

golden_aggregation_by_country = SparkSubmitOperator(
    task_id="golden_aggregation_by_country",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/aggregate_by_country.py",
    application_args=["/opt/data/silver", "/opt/data/gold/aggregated_by_country", "1"],
    dag=dag
)
end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Pipeline completed successfully"),
    dag=dag
)

(
    start
    >> bronze_layer
)

bronze_layer >> silver_internet
bronze_layer >> silver_sms_call
bronze_layer >> silver_user_events

silver_internet >> golden_aggregation_by_gridid
silver_sms_call >> golden_aggregation_by_gridid
silver_user_events >> golden_aggregation_by_gridid

silver_internet >> golden_aggregation_by_country
silver_sms_call >> golden_aggregation_by_country
silver_user_events >> golden_aggregation_by_country

golden_aggregation_by_gridid >> end
golden_aggregation_by_country >> end