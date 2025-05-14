from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def get_students_by_username(username: str):
    hook = PostgresHook(postgres_conn_id='iboux-production')
    query = """
        SELECT * 
        FROM student
        WHERE lc_username = %s
    """
    records = hook.get_records(query, parameters=(username,))
    cols = ['id', 'email', 'first_name', 'last_name', 'comm_language', 'lc_username', 'country', 'student_since', 'age_group', 'b2c_b2b', 'is_if', 'company_name', 'coach_name', 'account_manager_name', 'course1', 'course2', 'course3', 'company_id', 'is_group']
    df = pd.DataFrame(records, columns=cols)
    return df

def get_students_data_id_by_username(username: str):
    # TODO: change to production
    hook = PostgresHook(postgres_conn_id='dev-iboux')
    query = """
        SELECT id 
        FROM student_data
        WHERE lc_username = %s
    """
    records = hook.get_records(query, parameters=(username,))
    print(records)
    return records[0][0] if records else None


def insert_student_data(df: dict):
    # TODO: change to production
    hook = PostgresHook(postgres_conn_id='dev-iboux')
    query = """
        INSERT INTO student_data ( data)
        VALUES (%s)
        RERURNING id
    """
    hook.run(query, parameters=(df))
    return hook.get_first(query, parameters=(df,))[0]
    

def insert_course_data(course_data: dict, student_id: int):
    # TODO: change to production
    hook = PostgresHook(postgres_conn_id='dev-iboux')
    query = """
        INSERT INTO course_data (student_id, data)
        VALUES (%s, %s)
        RETURNING id
    """
    hook.run(query, parameters=(student_id, course_data))
    return hook.get_first(query, parameters=(course_data,)[0])