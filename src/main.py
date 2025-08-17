import os
from logger import logger
from unify import unify_data
from ingestion import load_data
from cleaning import data_cleaning, clean_course_details, clean_names_and_dob, clean_demographics, clean_attendance_dates
from transformation import data_transform
from quality_checks import data_quality_checks
from feature_engineering import run_feature_engineering_query

from clean_rtf_csvs import rtf_to_csv

# Import RTF cleaning helper
from clean_rtf_csvs import rtf_to_csv
from pathlib import Path

def print_sample_demographics(df, title=""):
    """Print first 25 rows of the demographics DataFrame with all columns."""
    print(f"\n{title}")
    print(df.head(45))

def main():
    logger.info("Starting Innovare Task Pipeline")

    # Paths
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_path = Path(base_path) / '..' / 'data'

    # Step 0: Clean any RTF-wrapped CSVs in place
    rtf_files = ["student_demographics.csv", "gradebook_export.csv", "attendance_records.csv"]
    for file_name in rtf_files:
        rtf_path = data_path / file_name
        if rtf_path.exists():
            rtf_to_csv(rtf_path, rtf_path)  # overwrite same file
        else:
            logger.warning(f"⚠️ File not found: {rtf_path}")

    # Step 1: Load data
    demographics_file = data_path / 'student_demographics.csv'
    grades_file = data_path / 'gradebook_export.csv'
    attendance_file = data_path / 'attendance_records.csv'

    demographics_df = load_data(demographics_file)
    grades_df = load_data(grades_file)
    attendance_df = load_data(attendance_file)

    # Print original names before cleaning
    print_sample_demographics(demographics_df, title="Demographics BEFORE cleaning")

    # Step 2: Clean data
    demographics_df = clean_names_and_dob(demographics_df)
    demographics_df = clean_demographics(demographics_df)
    demographics_df, grades_df, attendance_df = data_cleaning(demographics_df, grades_df, attendance_df)

    # Print demographics after cleaning
    print_sample_demographics(demographics_df, title="Demographics AFTER cleaning")

    # Step 3: Clean course_details
    grades_df = clean_course_details(grades_df)

    # Step 4: Transform data
    demographics_df, grades_df, attendance_df = data_transform(demographics_df, grades_df, attendance_df)

    # Step 5: Data quality checks
    demographics_df, grades_df, attendance_df, quality_passed = data_quality_checks(
        demographics_df, grades_df, attendance_df
    )

    print("\nCleaned Grades Data:")
    print(grades_df.head(45))

    print("\nCleaned Attendance Data:")
    print(attendance_df.head(45))

    if not quality_passed:
        logger.error("Pipeline terminated due to data quality failure")
        return

    # Step 6: Unify/join cleaned and transformed datasets
    logger.info("Starting to join demographics, grades, and attendance data")
    unified_df = unify_data(demographics_df, grades_df, attendance_df)
    logger.info(f"Unified data shape after joins: {unified_df.shape}")
    unified_df.columns = unified_df.columns.str.strip().str.replace(r'[^A-Za-z0-9_]', '_', regex=True)

    # Step 7: Load unified data to BigQuery
    project_id = 'bamboo-zone-468620-k8'  # Your GCP project ID
    table_id = 'raw_student_data.cleaned_merged_student_data'  # dataset.table format
    try:
        from bq_utils import load_to_bigquery
        load_to_bigquery(unified_df, table_id, project_id=project_id, if_exists='replace')
    except Exception as e:
        logger.error(f"Failed to load unified data to BigQuery: {e}")
        return

    # Step 8: Run feature engineering SQL query in BigQuery
    try:
        run_feature_engineering_query(failure_cutoff=60)
    except Exception as e:
        logger.error(f"Feature engineering step failed: {e}")
        return

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
