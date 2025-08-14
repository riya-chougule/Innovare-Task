from logger import logger
import pandas as pd

def data_quality_checks(demographics_df, grades_df, attendance_df):
    """
    Performs basic data quality validation on student datasets.

    Steps:
    1. Checks for duplicate records based on primary keys (entry_id, record_id).
    2. Logs a warning if duplicates are found.
    3. Returns cleaned datasets and a boolean flag indicating overall quality.
    """

    logger.info("Running data quality checks")

    quality_passed = True

    # Check duplicates in grades based on entry_id
    duplicates_grades = grades_df.duplicated(subset=['entry_id'], keep=False)
    if duplicates_grades.any():
        count = duplicates_grades.sum()
        logger.warning(f"Duplicate entry_id values found in Grades data: {count} duplicates")
        before_count = len(grades_df)
        grades_df = grades_df.drop_duplicates(subset=['entry_id'], keep='first')
        after_count = len(grades_df)
        logger.info(f"Removed {before_count - after_count} duplicate rows from Grades data")

    # Check duplicates in attendance based on record_id
    duplicates_attendance = attendance_df.duplicated(subset=['record_id'], keep=False)
    if duplicates_attendance.any():
        count = duplicates_attendance.sum()
        logger.warning(f"Duplicate record_id values found in Attendance data: {count} duplicates")
        before_count = len(attendance_df)
        attendance_df = attendance_df.drop_duplicates(subset=['record_id'], keep='first')
        after_count = len(attendance_df)
        logger.info(f"Removed {before_count - after_count} duplicate rows from Attendance data")

    if quality_passed:
        logger.info("All data quality checks passed")
    else:
        logger.error("Data quality checks failed")

    return demographics_df, grades_df, attendance_df, quality_passed
