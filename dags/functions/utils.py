import re
import pandas as pd
from typing import Optional, List

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

def extract_username(filename: str) -> str:
    match = re.search(r'(\S+)\.xlsx$', filename)
    if match:
        return match.group(1)
    return None



def get_course_language(df: pd.DataFrame) -> str:
    """
    Extract the language of the course from the TSV data.
    """
    if df is None or df.empty:
        raise ValueError("Input DataFrame cannot be empty or None")
        
    # Try filename 
    try:
        filename = df[0].get('filename')
        if filename:
            lang= filename[filename.find("[") + 2:filename.find("-")].strip()
            if lang:
                if lang in SUPPORTED_LANGUAGES:
                    return lang
    except (KeyError, AttributeError):
        pass

    # Try material_name
    try:
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
    
    return 'Unknown'


def get_customer_type(filename: str) -> str:
    """
    Extract the customer type from the filename.
    """
    if 'companies' in filename or 'company' in filename or 'b2b' in filename:
        return 'B2B'
    elif 'taas' in filename and 'b2c' not in filename and 'b2b' not in filename:
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
        
def is_if_course(filename: str) -> bool:
    """
    Check if the course is an IF course.
    """
    return 'if]' in filename or 'fundae' in filename

def get_course_data(df: pd.DataFrame) -> dict:
    """
    Extract course data from the TSV data.
    """
    filename = df[0].get('filename')
    language = get_course_language(df)
    class_length = df[len(df)].get('class_length')
    credits_qty = df[len(df)].get('class_number')
    customer_type = get_customer_type(filename.lower())
    if customer_type == 'TaaS':
        taas_school = get_taas_school(filename.lower())
    else:
        taas_school = None
    is_if = is_if_course(filename)
    course_data = {
        'language': language,
        'class_length': class_length,
        'credits_qty': credits_qty,
        'customer_type': customer_type,
        'taas_school': taas_school,
        'is_if': is_if,
        'spreadsheet_name': filename
    }
    return course_data
    



    if df is None or df.empty:
        raise ValueError("Input DataFrame cannot be empty or None")
