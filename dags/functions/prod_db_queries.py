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


def insert_student_data(student_data: dict):
    # TODO: change to production
    hook = PostgresHook(postgres_conn_id='dev-iboux')
    
    # Convert date to proper format
    if student_data.get('student_since'):
        student_data['student_since'] = pd.to_datetime(student_data['student_since'], format='%d-%m-%Y').strftime('%Y-%m-%d')
    
    query = """
        INSERT INTO student_data (
            email,
            first_name,
            last_name,
            comm_language,
            lc_username,
            country,
            student_since,
            age_group
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    parameters = tuple(student_data[field] for field in [
        'email', 'first_name', 'last_name',
        'comm_language', 'lc_username', 'country',
        'student_since', 'age_group'
    ])

    hook.run(query, parameters=parameters)
    inserted_id = hook.get_first("SELECT MAX(id) FROM student_data")[0]
    return inserted_id



def insert_course_data(course_data: dict, student_id: int):
    # TODO: change to production
    hook = PostgresHook(postgres_conn_id='dev-iboux')
    query = """
        INSERT INTO course_data (student_id, data)
        VALUES (%s, %s)
        RETURNING id
    """
    return hook.get_first(query, parameters=(student_id, course_data))[0]