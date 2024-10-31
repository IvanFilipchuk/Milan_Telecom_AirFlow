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

process_data = SparkSubmitOperator(
    task_id="process_data",
    conn_id="spark-conn",
    application="jobs/python/data_processing_job.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Completed successfully"),
    dag=dag
)
start >> process_data >> end
