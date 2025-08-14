from logger import logger
import pandas as pd

# ---------------------------------------------------------
# Helper functioms
# ---------------------------------------------------------

def normalize_student_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes student_id format across datasets.
    - Converts to string
    - Renoves leading/trailing spaces
    - Uppercases IDs for consistent matching
    """
    logger.info("Normalizing student_id")
    if 'student_id' in df.columns:
        df['student_id'] = df['student_id'].astype(str).str.strip().str.upper()
    return df


def extract_start_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures 'start_date' column (if present) is stored as a datetime object.
    - Invalid dates become NaT (Not a Time)
    """
    logger.info("Extracting start date")
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    return df


def handle_missing_values(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Standardizes and handles missing values for any dataset.
    Steps:
      1. Replace placeholders ('', 'NULL', 'N/A', 'na', 'NaN', '\') with proper NaN values.
      2. Strip whitespace from string columns.
      3. Replace 'nan'/'None' strings with NaN.
      4. Fill numeric NaN values with 0 (avoids breaking numeric calculations).
      5. Leave categorical NaNs for later handling (model training may require explicit missing indicators).
    Logs missing value counts before and after cleaning for debugging purposes.
    """
    logger.info(f"Handling missing values for {dataset_name}")

    # Replace placeholder values with NaN
    df = df.replace(['', 'NULL', 'N/A', 'na', 'NaN', '\\'], pd.NA)

    # Log missing counts before fixing
    missing_before = df.isna().sum()
    logger.info(f"Missing values before handling in {dataset_name}:\n{missing_before}")

    # Fill numeric NaNs with 0
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(0)

    # Clean string/object columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(['nan', 'None'], pd.NA)

    # Log missing counts after fixing
    missing_after = df.isna().sum()
    logger.info(f"Missing values after handling in {dataset_name}:\n{missing_after}")

    return df


# ---------------------------------------------------------
# Dataset-specific transformation functions
# ---------------------------------------------------------

def transform_demographics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and standardizes student demographics data.
    - Normalizes student IDs
    - Parses start dates if present
    - Handles missing values
    """
    logger.info("Transforming demographics data")
    df = normalize_student_id(df)
    df = extract_start_date(df)
    df = handle_missing_values(df, "demographics")
    return df


def convert_grade(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts letter grades to numeric values.
    - Direct numeric grades are kept as-is
    - Letter grades are mapped to the midpoint of their numeric range
    - Invalid/unrecognized grades become None
    """
    logger.info("Converting grade to numeric")

    grade_ranges = {
        'A+': (97, 100), 'A': (93, 96), 'A-': (90, 92),
        'B+': (87, 89), 'B': (83, 86), 'B-': (80, 82),
        'C+': (77, 79), 'C': (73, 76), 'C-': (70, 72),
        'D+': (67, 69), 'D': (63, 66), 'D-': (60, 62),
        'F': (0, 59)
    }

    def grade_to_numeric(grade):
        try:
            return float(grade)  # Numeric grades
        except (ValueError, TypeError):
            pass
        if isinstance(grade, str):
            grade = grade.strip().upper()
            if grade in grade_ranges:
                low, high = grade_ranges[grade]
                return (low + high) / 2  # midpoint
        return None

    df['grade_numeric'] = df['grade'].apply(grade_to_numeric)
    return df


def transform_grades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and standardizes student grade data.
    - Normalizes student IDs
    - Converts grades to numeric form
    - Handles missing values
    """
    logger.info("Transforming grades data")
    df = normalize_student_id(df)
    df = convert_grade(df)
    df = handle_missing_values(df, "grades")
    return df


def transform_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and standardizes student attendance data.
    - Normalizes student IDs
    - Parses start dates if present
    - Handles missing values
    """
    logger.info("Transforming attendance data")
    df = normalize_student_id(df)
    df = extract_start_date(df)
    df = handle_missing_values(df, "attendance")
    return df


# ---------------------------------------------------------
# Main transformation pipeline
# ---------------------------------------------------------

def data_transform(demographics_df: pd.DataFrame,
                   grades_df: pd.DataFrame,
                   attendance_df: pd.DataFrame):
    """
    Orchestrates the transformation process for all datasets.
    Steps for each dataset:
      1. Normalize student IDs
      2. Convert or parse necessary columns (dates, grades)
      3. Handle missing values
    Returns:
      (clean_demographics_df, clean_grades_df, clean_attendance_df)
    """
    logger.info("Starting data transformation pipeline")

    demographics_df = transform_demographics(demographics_df)
    grades_df = transform_grades(grades_df)
    attendance_df = transform_attendance(attendance_df)

    logger.info("Data transformation pipeline completed")
    return demographics_df, grades_df, attendance_df
