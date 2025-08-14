import os
from logger import logger
from unify import unify_data
from ingestion import load_data
from cleaning import data_cleaning, clean_course_details
from bq_utils import load_to_bigquery
from transformation import data_transform
from quality_checks import data_quality_checks
from feature_engineering import run_feature_engineering_query

def main():
    logger.info("Starting Innovare Task Pipeline")

    base_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_path, '..', 'data')

    demographics_file = os.path.join(data_path, 'student_demographics.csv')
    grades_file = os.path.join(data_path, 'gradebook_export.csv')
    attendance_file = os.path.join(data_path, 'attendance_records.csv')

    # Load data
    demographics_df = load_data(demographics_file)
    grades_df = load_data(grades_file)
    attendance_df = load_data(attendance_file)

    # Clean data
    demographics_df, grades_df, attendance_df = data_cleaning(demographics_df, grades_df, attendance_df)

    # Clean course_details
    grades_df = clean_course_details(grades_df)

    # Transform data
    demographics_df, grades_df, attendance_df = data_transform(demographics_df, grades_df, attendance_df)

    # Data quality checks
    demographics_df, grades_df, attendance_df, quality_passed = data_quality_checks(
        demographics_df, grades_df, attendance_df
    )

    print("#############  Cleaned Demographics Data:#############")
    print(demographics_df.head())

    print("\nCleaned Grades Data:")
    print(grades_df.head())

    print("\nCleaned Attendance Data:")
    print(attendance_df.head())

    if not quality_passed:
        logger.error("Pipeline terminated due to data quality failure")
        return

    # Unify/join cleaned and transformed datasets
    logger.info("Starting to join demographics, grades, and attendance data")
    unified_df = unify_data(demographics_df, grades_df, attendance_df)
    logger.info(f"Unified data shape after joins: {unified_df.shape}")

    # Load unified data to BigQuery
    project_id = 'bamboo-zone-468620-k8'  # Your GCP project ID
    table_id = 'raw_student_data.cleaned_merged_student_data'  # dataset.table format

    try:
        load_to_bigquery(unified_df, table_id, project_id=project_id, if_exists='replace')
    except Exception as e:
        logger.error(f"Failed to load unified data to BigQuery: {e}")
        return

    # Run feature engineering SQL query in BigQuery
    try:
        run_feature_engineering_query(failure_cutoff=60)
    except Exception as e:
        logger.error(f"Feature engineering step failed: {e}")
        return

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
