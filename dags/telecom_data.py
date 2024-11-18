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
    python_callable=lambda: print("Start"),
    dag=dag
)

bronze_layer = SparkSubmitOperator(
    task_id="bronze_layer",
    conn_id="spark-conn",
    application="jobs/python/data_processing_job.py",
    application_args=["/opt/data/data1.csv", "/opt/data/bronze/sample_data", "2"],
    dag=dag
)

silver_layer = SparkSubmitOperator(
    task_id="silver_layer",
    conn_id="spark-conn",
    application="jobs/python/data_processing_job_silver.py",
    dag=dag
)

golden_layer = SparkSubmitOperator(
    task_id="golden_layer",
    conn_id="spark-conn",
    application="jobs/python/data_processing_job_gold.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Completed successfully"),
    dag=dag
)
start >> bronze_layer>> silver_layer >> golden_layer >> end
