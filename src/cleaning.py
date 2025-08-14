from logger import logger
import pandas as pd

# --- Helper functions ---

import pandas as pd
import re

def clean_names_and_dob(df):
    """
    Cleans first_name, last_name, and DOB columns.
    Fixes cases where DOB is attached to the name.
    """

    # Iterate over each row
    for idx, row in df.iterrows():
        # Fix first_name if DOB is attached
        match = re.search(r'(\D+)(\d{4}-\d{2}-\d{2})', str(row['first_name']))
        if match:
            df.at[idx, 'first_name'] = match.group(1).strip()
            df.at[idx, 'DOB'] = match.group(2)

        # Fix last_name if DOB is attached
        match = re.search(r'(\D+)(\d{4}-\d{2}-\d{2})', str(row['last_name']))
        if match:
            df.at[idx, 'last_name'] = match.group(1).strip()
            df.at[idx, 'DOB'] = match.group(2)

    # Optional: strip whitespace and standardize capitalization
    df['first_name'] = df['first_name'].str.strip().str.title()
    df['last_name'] = df['last_name'].str.strip().str.title()

    return df


def clean_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize gender to 'M', 'F', or NaN."""
    logger.info("Cleaning gender column")
    if 'gender' in df.columns:
        df['gender'] = df['gender'].astype(str).str.upper().replace({'MALE':'M', 'FEMALE':'F','NO': pd.NA})
        df['gender'] = df['gender'].where(df['gender'].isin(['M','F']), pd.NA)
    return df

def clean_boolean_columns(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Standardize boolean columns with various truthy/falsey representations."""
    if col_name in df.columns:
        true_values = {'yes', 'true', 'y', '1'}
        false_values = {'no', 'false', 'n', '0'}
        df[col_name] = df[col_name].astype(str).str.lower().str.strip()
        df[col_name] = df[col_name].apply(
            lambda x: True if x in true_values else (False if x in false_values else pd.NA)
        )
    return df

def clean_date(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Convert date columns to datetime; invalid formats become NaT."""
    logger.info(f"Cleaning date column: {col_name}")
    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
    return df

def clean_student_id(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize student IDs and strip prefixes like SID- or S-."""
    logger.info("Cleaning student_id column")
    if 'student_id' in df.columns:
        df['student_id'] = df['student_id'].astype(str).str.upper()
        df['student_id'] = df['student_id'].str.replace(r'^(SID-|S-)', '', regex=True).str.strip()
    return df

def ensure_unique_student_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure student_id is unique by dropping duplicates."""
    if 'student_id' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['student_id'])
        after = len(df)
        if before != after:
            logger.warning(f"Dropped {before - after} duplicate student_id rows")
    return df

def data_summary(df: pd.DataFrame) -> None:
    """Log quick summary stats for the dataset."""
    logger.info(f"Data Summary:\n{df.describe(include='all')}")

# --- Cleaning for each dataset type ---

def clean_demographics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean demographic-specific columns for consistent formatting.
    Fix boolean/text inconsistencies, DOB formats, enrollment_status, and notes.
    """
    logger.info("Cleaning demographics dataset")

    # --- Standardize boolean/text columns ---
    bool_map = {'Yes': True, 'No': False, 'True': True, 'False': False}
    for col in ['ELL_Status', 'IEP_Status', 'FRL_Status']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
            df[col] = df[col].map(bool_map).fillna(pd.NA)

    # --- Standardize enrollment status ---
    if 'enrollment_status' in df.columns:
        df['enrollment_status'] = df['enrollment_status'].astype(str).str.strip().str.title()
        df['enrollment_status'] = df['enrollment_status'].replace({'\\': pd.NA, 'Nan': pd.NA, 'Na': pd.NA})

    # --- Standardize notes ---
    if 'notes' in df.columns:
        df['notes'] = df['notes'].astype(str).str.strip().replace({'\\': '', 'Nan': '', 'Na': ''})

    # --- Standardize DOB ---
    if 'DOB' in df.columns:
        df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce').dt.strftime('%Y-%m-%d')

    # --- Standardize gender ---
    df = clean_gender(df)

    # --- Clean student IDs ---
    df = clean_student_id(df)

    # --- Remove duplicates ---
    df = ensure_unique_student_ids(df)

    return df

# --- Course & grade cleaning ---
def clean_course_details(df: pd.DataFrame) -> pd.DataFrame:
    if 'course_details' in df.columns:
        df['course_details'] = df['course_details'].astype(str).str.replace(r"[\\']", "", regex=True)
        df['course_details'] = df['course_details'].str.replace(r"\s*-\s*", ": ", regex=True)
        df['course_details'] = df['course_details'].str.strip()
        df['course_details'] = df['course_details'].str.extract(r"course_name:\s*([^,]+)")[0].fillna(df['course_details'])
    return df

def clean_credits_and_grades(df: pd.DataFrame) -> pd.DataFrame:
    for col in ['credits_earned', 'grade']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.rstrip('\\')
    return df

def clean_gradebook(df: pd.DataFrame) -> pd.DataFrame:
    """Convert letter grades to numeric scores without dropping rows."""
    logger.info("Cleaning gradebook data with parameterized grade ranges")
    grade_ranges = {
        'A+': (97, 100), 'A':  (93, 96), 'A-': (90, 92),
        'B+': (87, 89), 'B':  (83, 86), 'B-': (80, 82),
        'C+': (77, 79), 'C':  (73, 76), 'C-': (70, 72),
        'D+': (67, 69), 'D':  (63, 66), 'D-': (60, 62),
        'F':  (0, 59)
    }

    def grade_to_numeric(grade):
        try:
            return float(grade)
        except (ValueError, TypeError):
            pass
        if isinstance(grade, str):
            grade = grade.strip().upper()
            if grade in grade_ranges:
                low, high = grade_ranges[grade]
                return (low + high) / 2
        return pd.NA  # Keep row even if grade is invalid

    df['grade_numeric'] = df['grade'].apply(grade_to_numeric)
    return df


# --- Attendance cleaning ---
def clean_attendance_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Fix attendance 'date' column, handling single dates and date ranges."""
    logger.info("Cleaning attendance date column")
    if 'date' in df.columns:
        # Convert to string and strip spaces
        df['date'] = df['date'].astype(str).str.strip()

        # Extract the first date in a possible range (YYYYMMDD or YYYY-MM-DD)
        df['date'] = df['date'].str.replace(r'\.0$', '', regex=True)
        df['date'] = df['date'].str.extract(r'(\d{8})(?:-\d{8})?')[0]

        # Parse into datetime format
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')

        # Count and warn about invalid dates
        num_invalid = df['date'].isna().sum()
        if num_invalid > 0:
            logger.warning(f"{num_invalid} attendance dates could not be parsed and set to NaT")

    return df




def clean_attendance_status(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up attendance reason text and replace blanks or 'NULL\' with NaN."""
    logger.info("Cleaning attendance reason column")
    if 'reason' in df.columns:
        # Convert to string, strip whitespace
        df['reason'] = df['reason'].astype(str).str.strip()

        # Replace empty strings, 'NULL', 'NULL\' or single backslashes with NaN
        df['reason'] = df['reason'].replace({'': pd.NA, 'NULL': pd.NA, 'NULL\\': pd.NA, '\\': pd.NA})

        # Remove any trailing backslashes from remaining values
        df['reason'] = df['reason'].str.rstrip('\\').replace({'': pd.NA})
    return df



# --- Master cleaning pipeline ---
def data_cleaning(demographics_df, grades_df, attendance_df):
    logger.info("Starting full data cleaning pipeline")

    # --- Demographics ---
    demographics_df = clean_names_and_dob(demographics_df)
    demographics_df = clean_demographics(demographics_df)  # unique student_ids enforced here

    # --- Grades ---
    grades_df = clean_credits_and_grades(grades_df)
    grades_df = clean_course_details(grades_df)
    grades_df = clean_gradebook(grades_df)
    grades_df = clean_student_id(grades_df)  # do NOT remove duplicates

    # --- Attendance ---
    attendance_df = clean_attendance_status(attendance_df)
    attendance_df = clean_attendance_dates(attendance_df)
    attendance_df = clean_student_id(attendance_df)  # do NOT remove duplicates

    logger.info("Data cleaning pipeline completed")
    return demographics_df, grades_df, attendance_df

