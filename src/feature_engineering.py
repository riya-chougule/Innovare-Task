from google.cloud import bigquery
from logger import logger

def run_feature_engineering_query(failure_cutoff: int):
    """
    Runs a BigQuery SQL query to create/update the student_features view.
    Aggregates credits, failures, attendance %, and behavior flags.

    Args:
        failure_cutoff (int): Numeric grade threshold to consider a course as failed.
    """
    logger.info(f"Running feature engineering query in BigQuery with failure cutoff = {failure_cutoff}")

    client = bigquery.Client()

    query = f"""
    CREATE OR REPLACE VIEW raw_student_data.student_features AS
    SELECT
      student_id,
      SUM(total_credits) AS credits_earned_semester,
      COUNTIF(avg_grade_numeric < {failure_cutoff}) AS core_course_failures,
      SAFE_DIVIDE(SUM(Present), NULLIF(SUM(Present) + SUM(Absent) + SUM(Tardy), 0)) AS attendance_percentage,
      MAX(CASE WHEN LOWER(notes) LIKE '%behavior_flag=true%' THEN 1 ELSE 0 END) AS behavioral_flag
    FROM
      raw_student_data.cleaned_student_data
    GROUP BY
      student_id;
    """

    try:
        query_job = client.query(query)
        query_job.result()  # Wait until the job finishes
        logger.info("Feature view updated successfully.")
    except Exception as e:
        logger.error(f"Feature engineering query failed: {e}")
        raise
