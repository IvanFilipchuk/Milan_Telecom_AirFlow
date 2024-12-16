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
bronze_layer_synthetic = SparkSubmitOperator(
    task_id="load_to_bronze_synthetic_layer",
    conn_id="spark-conn",
    application="jobs/python/bronze_layer/load_data_to_bronze.py",
    application_args=["/opt/data/data-generated", "/opt/data/bronze/sample_data", "2"],
    dag=dag
)


silver_internet = SparkSubmitOperator(
    task_id="silver_normalization_internet",
    conn_id="spark-conn",
    application="jobs/python/silver_layer/normalize_internet.py",
    application_args=["/opt/data/bronze/sample_data", "/opt/data/silver/internet", "2"],
    dag=dag
)
silver_internet_synthetic = SparkSubmitOperator(
    task_id="silver_normalization_internet_synthetic",
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
silver_sms_call_synthetic = SparkSubmitOperator(
    task_id="silver_normalization_sms_call_synthetic",
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
silver_user_events_synthetic = SparkSubmitOperator(
    task_id="silver_normalization_user_events_synthetic",
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
golden_aggregation_by_gridid_synthetic = SparkSubmitOperator(
    task_id="golden_aggregation_by_gridid_synthetic",
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
golden_aggregation_by_country_synthetic = SparkSubmitOperator(
    task_id="golden_aggregation_by_country_synthetic",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/aggregate_by_country.py",
    application_args=["/opt/data/silver", "/opt/data/gold/aggregated_by_country", "1"],
    dag=dag
)
# test_postgresql_connection = SparkSubmitOperator(
#     task_id="test_postgresql_connection",
#     conn_id="spark-conn",
#     application="jobs/python/test_postgresql_connection.py",
#     conf={
#         "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
#         "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
#         "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
#     },
#     dag=dag
# )

load_country_internet_to_db = SparkSubmitOperator(
    task_id="load_country_internet_to_db",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/country_internet.py",
    application_args=[
        "/opt/data/gold/aggregated_by_country/internet",
        "country_internet"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)
load_country_internet_to_db_synthetic = SparkSubmitOperator(
    task_id="load_country_internet_to_db_synthetic",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/country_internet.py",
    application_args=[
        "/opt/data/gold/aggregated_by_country/internet",
        "country_internet"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)





load_country_sms_call_to_db = SparkSubmitOperator(
    task_id="load_country_sms_call_to_db",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/country_sms_call.py",
    application_args=[
        "/opt/data/gold/aggregated_by_country/sms_call",
        "country_sms_call"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)
load_country_sms_call_to_db_synthetic = SparkSubmitOperator(
    task_id="load_country_sms_call_to_db_synthetic",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/country_sms_call.py",
    application_args=[
        "/opt/data/gold/aggregated_by_country/sms_call",
        "country_sms_call"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)




load_gridid_internet_to_db = SparkSubmitOperator(
    task_id="load_gridid_internet_to_db",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/gridid_internet.py",
    application_args=[
        "/opt/data/gold/aggregated_by_gridid/internet",
        "gridid_internet"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)
load_gridid_internet_to_db_synthetic = SparkSubmitOperator(
    task_id="load_gridid_internet_to_db_synthetic",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/gridid_internet.py",
    application_args=[
        "/opt/data/gold/aggregated_by_gridid/internet",
        "gridid_internet"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)




load_gridid_sms_call_to_db = SparkSubmitOperator(
    task_id="load_gridid_sms_call_to_db",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/gridid_sms_call.py",
    application_args=[
        "/opt/data/gold/aggregated_by_gridid/sms_call",
        "gridid_sms_call"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)
load_gridid_sms_call_to_db_synthetic = SparkSubmitOperator(
    task_id="load_gridid_sms_call_to_db_synthetic",
    conn_id="spark-conn",
    application="jobs/python/gold_layer/load_to_db/gridid_sms_call.py",
    application_args=[
        "/opt/data/gold/aggregated_by_gridid/sms_call",
        "gridid_sms_call"
    ],
    conf={
        "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.4.jar",
    },
    dag=dag
)



sinusoidal_function_generation = SparkSubmitOperator(
    task_id="sinusoidal_function_generation",
    conn_id="spark-conn",
    application="jobs/python/sinusoidal.py",
    application_args=["/opt/data/data1.csv","/opt/data/data-generated"],
    dag=dag)
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

golden_aggregation_by_gridid >> load_gridid_internet_to_db
golden_aggregation_by_gridid >> load_gridid_sms_call_to_db

golden_aggregation_by_country >> load_country_internet_to_db
golden_aggregation_by_country >> load_country_sms_call_to_db


load_gridid_internet_to_db >> sinusoidal_function_generation
load_gridid_sms_call_to_db >> sinusoidal_function_generation
load_country_internet_to_db >> sinusoidal_function_generation
load_country_sms_call_to_db >> sinusoidal_function_generation

sinusoidal_function_generation >> bronze_layer_synthetic
bronze_layer_synthetic >> silver_internet_synthetic
bronze_layer_synthetic >> silver_sms_call_synthetic
bronze_layer_synthetic >> silver_user_events_synthetic
silver_internet_synthetic >> golden_aggregation_by_gridid_synthetic
silver_sms_call_synthetic >> golden_aggregation_by_gridid_synthetic
silver_user_events_synthetic >> golden_aggregation_by_gridid_synthetic

silver_internet_synthetic >> golden_aggregation_by_country_synthetic
silver_sms_call_synthetic >> golden_aggregation_by_country_synthetic
silver_user_events_synthetic >> golden_aggregation_by_country_synthetic

golden_aggregation_by_gridid_synthetic >> load_gridid_internet_to_db_synthetic
golden_aggregation_by_gridid_synthetic >> load_gridid_sms_call_to_db_synthetic

golden_aggregation_by_country_synthetic >> load_country_internet_to_db_synthetic
golden_aggregation_by_country_synthetic >> load_country_sms_call_to_db_synthetic



load_gridid_internet_to_db_synthetic >> end
load_gridid_sms_call_to_db_synthetic >> end
load_country_internet_to_db_synthetic >> end
load_country_sms_call_to_db_synthetic >> end

