from logger import logger
import pandas as pd

# Helper functions used by transform_demographics, transform_grades, transform_attendance
def normalize_student_id(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizing student_id")
    df['student_id'] = df['student_id'].astype(str).str.strip().str.upper()
    return df

def extract_start_date(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Extracting start date")
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    return df

# Transform functions
def transform_demographics(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming demographics data")
    df = normalize_student_id(df)
    df = extract_start_date(df)
    return df

def convert_grade(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Converting grade to numeric")

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
    return df


def transform_attendance(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming attendance data")
    df = normalize_student_id(df)
    df = extract_start_date(df)
    return df

def transform_grades(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming grades data")
    df = normalize_student_id(df)
    df = convert_grade(df)
    return df

# Main transform function
def data_transform(demographics_df: pd.DataFrame,
                   grades_df: pd.DataFrame,
                   attendance_df: pd.DataFrame):
    logger.info("Starting data transformation pipeline")

    demographics_df = transform_demographics(demographics_df)
    grades_df = transform_grades(grades_df)
    attendance_df = transform_attendance(attendance_df)
    print(demographics_df.columns.tolist())

    logger.info("Data transformation pipeline completed")

    return demographics_df, grades_df, attendance_df