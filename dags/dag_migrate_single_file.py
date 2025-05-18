from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
from typing import List, Dict
from functions.utils import extract_username, get_course_data, get_classes_data
from functions.prod_db_queries import get_students_by_username, get_students_data_id_by_username, insert_student_data, insert_course_data, insert_classes_data
from uuid import UUID

with DAG(
    dag_id="migrate_single_file",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    @task
    def parse_tsv_to_df(**context):
        file_path = context["dag_run"].conf["file_path"]
        print(f"Attempting to read: {file_path}")
        df = pd.read_csv(file_path, sep='\t')
        df = df.astype(object).where(pd.notnull(df), None)
        print(df.columns)
        return df.to_dict(orient='records')
    
    @task 
    def extract_metadata(parsed_file: List[Dict]):
        filename = parsed_file[0].get('filename')
        username = extract_username(filename)
        return username, 

    @task 
    def sync_student_record(parsed_file: List[Dict]):
        filename = parsed_file[0].get('filename')
        username = extract_username(filename)
        student_id = get_students_data_id_by_username(username)
        
        if student_id is None:
            students_df = get_students_by_username(username)
            if students_df.empty:
                raise ValueError(f"No student found for: [ filename:{filename} | username: {username} ]")
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

    parsed_file = parse_tsv_to_df()
    student_id = sync_student_record(parsed_file)
    course_id = create_course(parsed_file, student_id)
    register_course_classes(parsed_file, course_id)
    
    