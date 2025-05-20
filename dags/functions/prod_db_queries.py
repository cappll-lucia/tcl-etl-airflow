from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from uuid import UUID

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
        INSERT INTO course (
            student_id, 
            course_language,
            class_length,
            credits_qty,
            customer_type,
            taas_school,
            is_if,
            spreadsheet_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    parameters = (student_id,) + tuple((course_data[field] for field in [
        'language', 'class_length', 'credits_qty', 'customer_type', 'taas_school', 'is_if', 'spreadsheet_name'
    ]))

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query, parameters)
    course_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return course_id

def insert_classes_data(classes_data: dict, course_id: UUID):

    print(f"Course ID: {course_id}")

    if not classes_data:
        print("No classes to insert")
        return

    hook = PostgresHook(postgres_conn_id='dev-iboux')

    print(f"File total: {len(classes_data)}")
    classes_data = [
        row for row in classes_data
        if any(row.get(k) for k in ['class_date', 'teacher_name', 'lesson_plan', 'level'])
    ]
    print(f"With data total: {len(classes_data)}")


    for c in classes_data:
        c["course_id"] = course_id

    columns = [
        "course_id", "class_number", "class_date", "teacher_name",
        "technical_instructions", "message_to_teacher", "lesson_plan",
        "cancellation", "technical_issues", "class_comments",
        "student_progress_comments", "material_name", "alternative_material",
        "level", "unit_number", "page_number", "homework",
        "lc_notes_open", "compensation", "family_member_name"
    ]

    values = [
        tuple(c.get(col) for col in columns)
        for c in classes_data
    ]
    
    print(f"course_id: {course_id}")
    print("Ejemplo fila:", classes_data[0])

    hook = PostgresHook(postgres_conn_id='dev-iboux')
    hook.insert_rows(table="class", rows=values, target_fields=columns)

    
