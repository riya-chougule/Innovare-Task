from google.cloud import bigquery
from logger import logger

def run_feature_engineering_query():
    logger.info("Starting feature engineering SQL execution in BigQuery")

    client = bigquery.Client()

    query = """
    CREATE OR REPLACE VIEW raw_student_data.student_features AS
    SELECT
      student_id,
      SUM(total_credits) AS credits_earned_semester,
      COUNTIF(avg_grade_numeric < 65) AS core_course_failures,
      SAFE_DIVIDE(SUM(Present), NULLIF(SUM(Present) + SUM(Absent) + SUM(Tardy), 0)) AS attendance_percentage,
      MAX(CASE WHEN LOWER(notes) LIKE '%behavior_flag=true%' THEN 1 ELSE 0 END) AS behavioral_flag
    FROM
      raw_student_data.cleaned_student_data
    GROUP BY
      student_id;
    """

    try:
        query_job = client.query(query)
        query_job.result()  # Wait for job to complete
        logger.info("Feature engineering SQL executed successfully, view updated.")
    except Exception as e:
        logger.error(f"Failed to run feature engineering SQL: {e}")
        raise
