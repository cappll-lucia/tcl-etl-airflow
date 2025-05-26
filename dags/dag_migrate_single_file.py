from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
from typing import List, Dict
from functions.utils import extract_username, get_course_data, get_classes_data, extract_student_names
from functions.prod_db_queries import get_students_by_username, get_students_data_id_by_username, insert_student_data, insert_course_data, insert_classes_data, insert_raw_student
from uuid import UUID
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import io

with DAG(
    dag_id="migrate_single_file",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    @task
    def parse_tsv_from_gcs(**context):
        bucket = context["dag_run"].conf["bucket_name"]
        object_path = context["dag_run"].conf["object_path"]
        print(f"+++++ Processing gs://{bucket}/{object_path}")
        gcs_hook = GCSHook(gcp_conn_id="gcs-systems-acc")
        file_bytes = gcs_hook.download(bucket_name=bucket, object_name=object_path)

        df = pd.read_csv(io.StringIO(file_bytes.decode("utf-8")), sep="\t")
        df = df.astype(object).where(pd.notnull(df), None)
        print(f"Parsed from gs://{bucket}/{object_path} → Columns: {df.columns}")
        return df.to_dict(orient="records")
    

    @task 
    def extract_metadata(parsed_file: List[Dict]):
        filename_path = parsed_file[0].get('filename_path')
        username = extract_username(filename_path)
        return username, 


    @task 
    def sync_student_record(parsed_file: List[Dict]):
        filename_path = parsed_file[0].get('filename_path')
        username = extract_username(filename_path)
        student_id = get_students_data_id_by_username(username)
        print("student idddd--> ", student_id)
        
        if student_id is None:
            students_df = get_students_by_username(username)
            if students_df.empty:
                df = pd.DataFrame(parsed_file)
                first_name, last_name = extract_student_names(df)
                student_id = insert_raw_student(username=username, first_name=first_name, last_name=last_name)
            
            else:
                data = {
                    'email': students_df.iloc[0]['email'],
                    'first_name': students_df.iloc[0]['first_name'],
                    'last_name': students_df.iloc[0]['last_name'],
                    'comm_language': students_df.iloc[0]['comm_language'],
                    'lc_username': students_df.iloc[0]['lc_username'],
                    'country': students_df.iloc[0]['country'],
                    'student_since': students_df.iloc[0]['student_since'],
                    'age_group': students_df.iloc[0]['age_group'],
                }
                student_id = insert_student_data(data)
        
        if student_id is None:
            raise ValueError(f"No student found for: [ filename_path:{filename_path} | username: {username} ]")

        return student_id
    

    @task 
    def create_course(parsed_file: List[Dict], student_id: int):
        df = pd.DataFrame(parsed_file)
        course_data = get_course_data(df)
        course_id = insert_course_data(course_data, student_id)
        print(f"++++++ Course ID: {course_id}")
        return course_id
    

    @task
    def register_course_classes(parsed_file: List[Dict], course_id: UUID):
        df = pd.DataFrame(parsed_file)
        classes_data = get_classes_data(df)
        insert_classes_data(classes_data, course_id)


    @task
    def mark_file_as_done(**context):
        bucket= context['dag_run'].conf['bucket_name']
        object_path=context['dag_run'].conf['object_path']
        gcs_hook = GCSHook(gcp_conn_id='gcs-systems-acc')
        done_path = f"{object_path}.done"

        gcs_hook.copy(
            source_bucket=bucket,
            source_object=object_path,
            destination_bucket=bucket,
            destination_object=done_path
        )
        gcs_hook.delete(bucket_name=bucket, object_name=object_path)
        print(f"DONE {object_path} → {done_path}")
        

    parsed_file = parse_tsv_from_gcs()
    student_id = sync_student_record(parsed_file)
    course_id = create_course(parsed_file, student_id)
    register_course_classes(parsed_file, course_id) >> mark_file_as_done()

