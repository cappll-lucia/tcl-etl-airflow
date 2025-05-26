from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

def test_gcs_conn():
    hook = GCSHook(gcp_conn_id="gcs-systems-acc")
    files = hook.list(bucket_name="tcl-testing")
    print("Archivos encontrados:", files)

with DAG("test_gcs_connection", start_date=datetime(2025, 1, 1), schedule_interval=None, catchup=False) as dag:
    test = PythonOperator(
        task_id="test_gcs",
        python_callable=test_gcs_conn
    )