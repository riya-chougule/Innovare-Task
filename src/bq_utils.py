import pandas_gbq
from logger import logger

def load_to_bigquery(df, table_id: str, project_id: str, if_exists='replace'):
    """
    Uploads the DataFrame `df` to BigQuery table `table_id` using pandas_gbq.

    Args:
        df (pd.DataFrame): Data to upload.
        table_id (str): Table identifier in 'dataset.table' format.
        project_id (str): GCP project ID.
        if_exists (str): 'replace' to overwrite or 'append' to add data.
    """
    logger.info(f"Loading data to BigQuery table {table_id} in project {project_id}")

    try:
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            if_exists=if_exists,
            progress_bar=True
        )
        logger.info(f"Loaded {len(df)} rows to {table_id}")
    except Exception as e:
        logger.error(f"Failed to load data to BigQuery: {e}")
        raise
