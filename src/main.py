from logger import logger
from ingestion import load_data
from cleaning import data_cleaning
from transformation import data_transform
from quality_checks import data_quality_checks
from feature_engineering import run_feature_engineering_query
import os

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

    # Transform data
    demographics_df, grades_df, attendance_df = data_transform(demographics_df, grades_df, attendance_df)

    # Data quality checks
    demographics_df, grades_df, attendance_df, quality_passed = data_quality_checks(demographics_df, grades_df, attendance_df)

    if not quality_passed:
        logger.error("Pipeline terminated due to data quality failure")
        return

    # Run feature engineering SQL query in BigQuery
    try:
        run_feature_engineering_query()
    except Exception as e:
        logger.error(f"Feature engineering step failed: {e}")
        return

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
