from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timezone
import hashlib

BUCKET_NAME = 'tcl-testing'
PREFIX_ROUTE='test5/'

def generate_run_id(file_path: str) -> str:
    clean_hash = hashlib.md5(file_path.encode()).hexdigest()[:10]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"auto__{clean_hash}__{timestamp}"

with DAG(
    dag_id='trigger_miration_from_tcl',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['migration', 'gcl']
) as dag:


    start = EmptyOperator(task_id='start')

    @task
    def fetch_unprocessed_files(bucket_name: str, prefix: str = '') -> list:
        gcs_hook = GCSHook(gcp_conn_id='gcs-systems-acc')
        all_files = gcs_hook.list(bucket_name=bucket_name, prefix=prefix)

        done_files = {f[:-5] for f in all_files if f.endswith(".done")}
        pending_files = [f for f in all_files if f.endswith(".tsv") and f not in done_files]

        return [{"bucket_name": bucket_name, "object_path": f} for f in pending_files[:1000]]

    files_to_process = fetch_unprocessed_files(bucket_name=BUCKET_NAME, prefix=PREFIX_ROUTE)

    trigger_dags = TriggerDagRunOperator.partial(
        task_id='trigger_migrate_single_file',
        trigger_dag_id='migrate_single_file'
    ).expand(conf=files_to_process)

    end = EmptyOperator(task_id='end')

    start >> files_to_process >> trigger_dags >> end


