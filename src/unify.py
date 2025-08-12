from logger import logger
import pandas as pd

def unify_data(demographics_df: pd.DataFrame,
               grades_df: pd.DataFrame,
               attendance_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting to join demographics, grades, and attendance data")

    # Ensure all student_id columns are consistent and uppercase
    for df in [demographics_df, grades_df, attendance_df]:
        df['student_id'] = df['student_id'].astype(str).str.strip().str.upper()

    # Left join grades with demographics on student_id
    demo_grades_df = pd.merge(demographics_df, grades_df, on='student_id', how='left', suffixes=('_demo', '_grade'))

    # Then join attendance on student_id
    unified_df = pd.merge(demo_grades_df, attendance_df, on='student_id', how='left', suffixes=('', '_attendance'))

    logger.info(f"Unified data shape after joins: {unified_df.shape}")

    return unified_df
