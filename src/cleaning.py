from logger import logger
import pandas as pd

# --- Helper functions ---

def clean_ell_status(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize ELL status to boolean values."""
    logger.info("Cleaning ELL status")
    if 'ell_status' in df.columns:
        df['ell_status'] = df['ell_status'].str.lower().map({'yes': True, 'no': False}).fillna(False)
    return df

def clean_frl_status(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize FRL (Free/Reduced Lunch) status to boolean values."""
    logger.info("Cleaning FRL status")
    if 'frl_status' in df.columns:
        df['frl_status'] = df['frl_status'].str.lower().map({'yes': True, 'no': False}).fillna(False)
    return df

def clean_date(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Convert date columns to datetime; invalid formats become NaT."""
    logger.info(f"Cleaning date column: {col_name}")
    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
    return df

def data_summary(df: pd.DataFrame) -> None:
    """Log quick summary stats for the dataset."""
    logger.info(f"Data Summary:\n{df.describe(include='all')}")

# --- Cleaning for each dataset type ---

def clean_demographics(df: pd.DataFrame) -> pd.DataFrame:
    """Clean ELL/FRL flags and date of birth in demographics data."""
    logger.info("Cleaning demographics data")
    df = clean_ell_status(df)
    df = clean_frl_status(df)
    df = clean_date(df, 'DOB')
    data_summary(df)
    return df

def clean_course_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans 'course_details':
    - Removes backslashes and quotes
    - Standardizes separators
    - Extracts main course name from dictionary-like strings
    """
    if 'course_details' in df.columns:
        df['course_details'] = df['course_details'].astype(str)
        df['course_details'] = df['course_details'].str.replace(r"[\\']", "", regex=True)
        df['course_details'] = df['course_details'].str.replace(r"\s*-\s*", ": ", regex=True)
        df['course_details'] = df['course_details'].str.strip()
        
        # Extract main course name if it looks like a dictionary
        df['course_details'] = df['course_details'].str.extract(r"course_name:\s*([^,]+)")[0].fillna(df['course_details'])
        
    return df


def clean_credits_and_grades(df: pd.DataFrame) -> pd.DataFrame:
    """Remove trailing slashes/whitespace from credits and grades."""
    for col in ['credits_earned', 'grade']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.rstrip('\\')
    return df

def clean_gradebook(df: pd.DataFrame) -> pd.DataFrame:
    """Convert letter grades to numeric scores; drop rows with invalid grades."""
    logger.info("Cleaning gradebook data with parameterized grade ranges")
    grade_ranges = {
        'A+': (97, 100), 'A':  (93, 96), 'A-': (90, 92),
        'B+': (87, 89), 'B':  (83, 86), 'B-': (80, 82),
        'C+': (77, 79), 'C':  (73, 76), 'C-': (70, 72),
        'D+': (67, 69), 'D':  (63, 66), 'D-': (60, 62),
        'F':  (0, 59)
    }

    def grade_to_numeric(grade):
        """Convert numeric grades or map letter grades to range midpoints."""
        try:
            return float(grade)
        except (ValueError, TypeError):
            pass
        if isinstance(grade, str):
            grade = grade.strip().upper()
            if grade in grade_ranges:
                low, high = grade_ranges[grade]
                return (low + high) / 2
        return None

    df['grade_numeric'] = df['grade'].apply(grade_to_numeric)
    before_rows = len(df)
    df = df.dropna(subset=['grade_numeric'])
    after_rows = len(df)
    logger.info(f"Dropped {before_rows - after_rows} rows due to invalid grades")
    return df

def clean_student_id(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize student IDs and strip prefixes like SID- or S-."""
    logger.info("Cleaning student_id column")
    if 'student_id' in df.columns:
        df['student_id'] = df['student_id'].astype(str).str.upper()
        df['student_id'] = df['student_id'].str.replace(r'^(SID-|S-)', '', regex=True).str.strip()
    return df

def clean_attendance_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Fix attendance date formats and convert to datetime."""
    logger.info("Cleaning attendance date column")
    if 'date' in df.columns:
        def fix_date_str(date_val):
            date_str = str(date_val).split('.')[0]
            if len(date_str) == 7:
                date_str = '20' + date_str[1:]
            return date_str

        df['date'] = df['date'].apply(fix_date_str)
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
    return df

def clean_attendance_status(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up attendance reason text and replace blanks with NaN."""
    logger.info("Cleaning attendance reason column")
    if 'reason' in df.columns:
        df['reason'] = df['reason'].astype(str).str.strip().str.rstrip('\\').replace({'': None, 'NULL': None})
        df['reason'] = df['reason'].replace({None: pd.NA})
    return df

# --- Master cleaning pipeline ---

def data_cleaning(demographics_df: pd.DataFrame,
                  grades_df: pd.DataFrame,
                  attendance_df: pd.DataFrame):
    """Run the full cleaning process for demographics, grades, and attendance."""
    logger.info("Starting data cleaning pipeline")
    demographics_df = clean_demographics(demographics_df)
    grades_df = clean_credits_and_grades(grades_df)
    grades_df = clean_course_details(grades_df)
    grades_df = clean_gradebook(grades_df)
    attendance_df = clean_attendance_status(attendance_df)
    attendance_df = clean_attendance_dates(attendance_df)

    # Normalize student IDs in all datasets
    demographics_df = clean_student_id(demographics_df)
    grades_df = clean_student_id(grades_df)
    attendance_df = clean_student_id(attendance_df)

    logger.info("Data cleaning pipeline completed")
    return demographics_df, grades_df, attendance_df
