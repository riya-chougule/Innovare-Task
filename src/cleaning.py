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

def clean_date(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    logger.info(f"Cleaning date column: {date_column}")
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    return df

def clean_attendance_status(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning attendance status")
    if 'attendance_status' in df.columns:
        df['attendance_status'] = df['attendance_status'].str.lower().map({'present': True, 'absent': False}).fillna(False)
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
def data_cleaning(demographics_df: pd.DataFrame,
                  grades_df: pd.DataFrame,
                  attendance_df: pd.DataFrame):
    logger.info("Starting data cleaning pipeline")

    demographics_df = clean_demographics(demographics_df)
    grades_df = clean_gradebook(grades_df)
    attendance_df = clean_attendance_status(attendance_df)

    logger.info("Data cleaning pipeline completed")

    return demographics_df, grades_df, attendance_df