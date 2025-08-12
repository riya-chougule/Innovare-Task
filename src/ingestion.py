from logger import logger
import pandas as pd

def load_data(file_path: str) -> pd.DataFrame:
    logger.info(f"Loading data from {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded data shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data from {file_path}: {e}")
        raise
