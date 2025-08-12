from logger import logger
import pandas as pd

# Helper functions called by cleaning modules
def clean_ell_status(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning ELL status")
    # Example: standardize ELL column values
    if 'ell_status' in df.columns:
        df['ell_status'] = df['ell_status'].str.lower().map({'yes': True, 'no': False}).fillna(False)
    return df

def clean_frl_status(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning FRL status")
    if 'frl_status' in df.columns:
        df['frl_status'] = df['frl_status'].str.lower().map({'yes': True, 'no': False}).fillna(False)
    return df

def clean_date(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    logger.info(f"Cleaning date column: {col_name}")
    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
    return df

def data_summary(df: pd.DataFrame) -> None:
    logger.info(f"Data Summary:\n{df.describe(include='all')}")

# Cleaning functions for each data category
def clean_demographics(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning demographics data")
    df = clean_ell_status(df)
    df = clean_frl_status(df)
    df = clean_date(df, 'DOB')
    data_summary(df)
    return df

def clean_credits_and_grades(df: pd.DataFrame) -> pd.DataFrame:
    for col in ['credits_earned', 'grade']:
        if col in df.columns:
            # Strip trailing backslash and whitespace
            df[col] = df[col].astype(str).str.strip().str.rstrip('\\')
    return df

def clean_gradebook(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning gradebook data with parameterized grade ranges")

    # Define grade ranges in a parameterized way
    grade_ranges = {
        'A+': (97, 100),
        'A':  (93, 96),
        'A-': (90, 92),
        'B+': (87, 89),
        'B':  (83, 86),
        'B-': (80, 82),
        'C+': (77, 79),
        'C':  (73, 76),
        'C-': (70, 72),
        'D+': (67, 69),
        'D':  (63, 66),
        'D-': (60, 62),
        'F':  (0, 59)
    }

    def grade_to_numeric(grade):
        # If it's already numeric, return as float
        try:
            return float(grade)
        except (ValueError, TypeError):
            pass

        # If it's a letter grade, take the midpoint of the range
        if isinstance(grade, str):
            grade = grade.strip().upper()
            if grade in grade_ranges:
                low, high = grade_ranges[grade]
                return (low + high) / 2
        return None  # Unrecognized format

    # Apply transformation
    df['grade_numeric'] = df['grade'].apply(grade_to_numeric)

    # Drop rows where grade could not be parsed
    before_rows = len(df)
    df = df.dropna(subset=['grade_numeric'])
    after_rows = len(df)

    logger.info(f"Dropped {before_rows - after_rows} rows due to invalid grades")
    return df 


# Main cleaning function
def clean_student_id(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning student_id column")
    if 'student_id' in df.columns:
        # Remove prefixes like 'SID-' or 'S-' and keep numeric part
        df['student_id'] = df['student_id'].astype(str).str.upper()  # Normalize case
        df['student_id'] = df['student_id'].str.replace(r'^(SID-|S-)', '', regex=True).str.strip()
    return df

def clean_attendance_dates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning attendance date column")
    if 'date' in df.columns:
        # Convert numeric string like 20250903.0 or 0250903 to datetime
        
        def fix_date_str(date_val):
            # Convert to string, strip decimals if any
            date_str = str(date_val).split('.')[0]
            
            # If length is 7 (e.g. 0250903), fix by inserting '20' at start
            if len(date_str) == 7:
                # e.g. 0250903 → '0' + '25' + '09' + '03' -> we want '20250903'
                date_str = '20' + date_str[1:]
            
            return date_str
        
        df['date'] = df['date'].apply(fix_date_str)
        
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
    return df

def clean_attendance_status(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning attendance reason column")
    if 'reason' in df.columns:
        # Strip whitespace and trailing backslashes
        df['reason'] = df['reason'].astype(str).str.strip().str.rstrip('\\').replace({'': None, 'NULL': None})
        # Replace empty strings or 'None' with NaN for consistency
        df['reason'] = df['reason'].replace({None: pd.NA})
    return df

# Then in your main cleaning function for attendance:
def data_cleaning(demographics_df: pd.DataFrame,
                  grades_df: pd.DataFrame,
                  attendance_df: pd.DataFrame):
    logger.info("Starting data cleaning pipeline")

    demographics_df = clean_demographics(demographics_df)
    
    # Clean trailing backslashes in credits_earned and grade columns before parsing grades
    grades_df = clean_credits_and_grades(grades_df)
    grades_df = clean_gradebook(grades_df)
    
    attendance_df = clean_attendance_status(attendance_df)
    attendance_df = clean_attendance_dates(attendance_df)

    # Clean student_id in all datasets after cleaning
    demographics_df = clean_student_id(demographics_df)
    grades_df = clean_student_id(grades_df)
    attendance_df = clean_student_id(attendance_df)

    logger.info("Data cleaning pipeline completed")

    return demographics_df, grades_df, attendance_df
