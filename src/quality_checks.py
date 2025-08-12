from logger import logger
import pandas as pd

def data_quality_checks(demographics_df, grades_df, attendance_df):
    logger.info("Running data quality checks")

    quality_passed = True

    # Check duplicates in grades
    duplicates_grades = grades_df.duplicated(subset=['student_id'], keep=False)
    if duplicates_grades.any():
        count = duplicates_grades.sum()
        logger.warning(f"Duplicate student_id values found in Grades data: {count} duplicates")
        before_count = len(grades_df)
        grades_df = grades_df.drop_duplicates(subset=['student_id'], keep='first')
        after_count = len(grades_df)
        logger.info(f"Removed {before_count - after_count} duplicate rows from Grades data")
        # Depending on policy, we could also mark quality_passed = False here if duplicates are critical

    # Check duplicates in attendance
    duplicates_attendance = attendance_df.duplicated(subset=['student_id'], keep=False)
    if duplicates_attendance.any():
        count = duplicates_attendance.sum()
        logger.warning(f"Duplicate student_id values found in Attendance data: {count} duplicates")
        before_count = len(attendance_df)
        attendance_df = attendance_df.drop_duplicates(subset=['student_id'], keep='first')
        after_count = len(attendance_df)
        logger.info(f"Removed {before_count - after_count} duplicate rows from Attendance data")
        # quality_passed = False if duplicates considered critical

    # Here you can add any other quality checks, setting quality_passed = False if needed

    if quality_passed:
        logger.info("All data quality checks passed")
    else:
        logger.error("Data quality checks failed")

    return demographics_df, grades_df, attendance_df, quality_passed
