import re
import pandas as pd
from typing import Optional, List
from datetime import datetime

SUPPORTED_LANGUAGES = {
    'EN': ['English', 'EN'],
    'ES': ['Spanish', 'ES'],
    'FR': ['French', 'FR'],
    'IT': ['Italian', 'IT'],
    'DE': ['German', 'DE']
}

TAAS_SCHOOLS = [
    'Academy Aziendali',
    'Au Pays des Langues',
    'Babbel folders',
    'BABBEL STUDENTS',
    'EUREKA Students',
    'INSTITUTO EUROPEO DE FORMACIÓN',
    'Language Link',
    'LIC FORMATION',
    'Pro English Courses',
    'Salt Idiomes',
    'UDIMA/CEF'
]

COACHES = [
    'Brittany - ',
    'Travis - ',
    'Alma - ',
    'Katja - ',
    'Ksenija - ',
    'Marie - ',
    'Maxime - ',
    'Maxime - ',
    'Laure - ',
    'Velina - ',
]

def extract_username(filename: str) -> str:
    match = re.search(r'(\S+)\.xlsx$', filename)
    if match:
        return match.group(1)
    return None

def extract_student_names(df: pd.DataFrame):
    if df is None or df.empty:
        raise ValueError("Input DataFrame cannot be empty or None")
            
    name = df.iloc[0]['student_name'] if 'student_name' in df.columns else None
    if not name:
        raise ValueError("Student name not found in DataFrame")
    
    name_parts = name.split()
    if len(name_parts) < 2:
        raise ValueError("Student name must contain at least first and last name")
    
    last_name = name_parts[-1]
    first_name = ' '.join(name_parts[:-1])
    
    return first_name, last_name



def get_course_language(df: pd.DataFrame) -> str:
    """
    Extract the language of the course from the TSV data.
    """
    if df is None or df.empty:
        raise ValueError("Input DataFrame cannot be empty or None")
        
    try:
        if 'filename' in df.columns:
            filename = df.iloc[0]['filename']
            if filename:
                lang = filename[filename.find("[") + 2:filename.find("-")].strip()
                if lang and lang in SUPPORTED_LANGUAGES:
                    return lang
    except (KeyError, AttributeError, IndexError):
        pass

    try:
        if 'material_name' in df.columns:
            material_names = df['material_name'].dropna()
            if not material_names.empty:
                first_material = material_names.iloc[0]
                if ' - ' in first_material:
                    lang = first_material.split(' - ')[0]
                    if lang in SUPPORTED_LANGUAGES:
                        return lang
                
                for material in material_names:
                    if ' - ' in material:
                        lang = material.split(' - ')[0]
                        if lang in SUPPORTED_LANGUAGES:
                            return lang
    except (KeyError, AttributeError):
        pass

    # Try in other columns
    try:
        for col in ['technical_instructions', 'message_to_teacher', 'class_comments']:
            if col in df.columns:
                text = ' '.join(df[col].dropna().astype(str)).lower()
                for lang_code, keywords in SUPPORTED_LANGUAGES.items():
                    if any(keyword.lower() in text for keyword in keywords):
                        return lang_code
    except (KeyError, AttributeError):
        pass
    
    return '-'





def get_customer_type(filename: str) -> str:
    """
    Extract the customer type from the filename.
    """
    if ('companies' in filename or 'company' in filename or 'b2b]' in filename) and 'b2c]' not in filename:
        return 'B2B'
    elif 'taas' in filename and 'b2c]' not in filename and 'b2b]' not in filename:
        return 'TaaS'
    else: 
        return 'B2C'


def get_taas_school(filename: str) -> str:
    """
    Extract the TAAS school from the filename.
    """
    for school in TAAS_SCHOOLS:
        if school.lower() in filename:
            return school
    return None


def get_company_name(filename: str)-> str:
    """
    Extract the company name from the filename.
    Expected format: ...COACHNAME - COMPANY_NAME___...
    """
    try:
        # Find the position after "travis - "
        start_idx = filename.lower().find("travis - ")
        if start_idx == -1:
            return None
            
        start_idx += len("travis - ")
        
        end_idx = filename.find("___", start_idx)
        if end_idx == -1:
            return None

        company_name = filename[start_idx:end_idx].strip()
        return company_name if company_name else None
    except Exception:
        return None

        
def is_if_course(filename: str) -> bool:
    """
    Check if the course is an IF course.
    """
    return 'if]' in filename or 'fundae' in filename
""



def get_course_data(df: pd.DataFrame) -> dict:
    """
    Extract course data from the TSV data.
    """
    if df is None or df.empty:
        raise ValueError("Input DataFrame cannot be empty or None")
        
    # Get filename from the first row
    filename = df.iloc[0]['filename'] if 'filename' in df.columns else None
    if not filename:
        raise ValueError("Filename not found in DataFrame")
        
    language = get_course_language(df)
    
    raw_class_length = df.iloc[-1]['class_length'] if 'class_length' in df.columns else None
    class_length = {0.5: '30-min', 1: '1-hour'}.get(raw_class_length, '-')

    credits_qty = int(df.iloc[-1]['class_number']) if 'class_number' in df.columns and df.iloc[-1]['class_number'] is not None else 0
    
    customer_type = get_customer_type(filename.lower())
    if customer_type == 'TaaS':
        taas_school = get_taas_school(filename.lower())
    else:
        taas_school = None

    if customer_type == 'B2B' or ' - Staff___' in filename:
        company_name = get_company_name(filename.lower())
    else:
        company_name = None



    is_if = is_if_course(filename)
    course_data = {
        'language': language,
        'class_length': class_length,
        'credits_qty': credits_qty,
        'customer_type': customer_type,
        'taas_school': taas_school,
        'company_name': company_name,
        'is_if': is_if,
        'spreadsheet_name': filename.replace('.xlsx', '')
    }
    print("course_data", course_data)
    return course_data



def get_classes_data(df: pd.DataFrame) -> dict:
    """
    Extract classes data from the TSV data.
    Each dict in the returned list corresponds to one row/class.
    """
    if df is None or df.empty:
        raise ValueError("Input DataFrame cannot be empty or None")
    
    classes_data = []
    
    for _, row in df.iterrows():
        try:
            day = int(row.get('day', 1))
            month = int(row.get('month', 1))
            year = int(row.get('year', 2024))
            class_date = datetime(year, month, day).date()
        except (ValueError, TypeError):
            class_date = None

        cancellation= row.get('no_show') or row.get('cancellation_24_hours')

        class_data = {
            'class_number': row.get('class_number'),
            'class_length': row.get('class_length'),
            'class_date': class_date,
            'teacher_name': row.get('teacher'),
            'technical_instructions': row.get('technical_instructions'),
            'message_to_teacher': row.get('message_to_teacher'),
            'lesson_plan': row.get('lesson_plan'),
            'cancellation': cancellation,
            'technical_issues': row.get('technical_issues'),
            'class_comments': row.get('class_comments'),
            'student_progress_comments': row.get('student_progress_comments'),
            'material_name': row.get('material_name'),
            'alternative_material': row.get('alternative_material'),
            'level': row.get('level'),
            'unit_number': str(row.get('unit_number')) if row.get('unit_number') is not None else None,
            'page_number': str(row.get('page_number')) if row.get('page_number') is not None else None,
            'homework': row.get('homework'),
            'lc_notes_open': row.get('lc_notes_open'),
            'compensation': row.get('compensation'),
            'family_member_name': None
        }
        classes_data.append(class_data)
    
    return classes_data

