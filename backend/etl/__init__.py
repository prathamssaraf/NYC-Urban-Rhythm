# This file makes the etl directory a Python package
# It also provides common utilities for ETL scripts

import os
import logging
from datetime import datetime, timedelta
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)

def get_default_date_range():
    """
    Returns a default date range of the past 7 days
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    return start_date, end_date

def save_to_csv(df, data_type, date_str=None):
    """
    Save a DataFrame to a CSV file in the data directory
    
    Args:
        df: pandas DataFrame to save
        data_type: type of data (e.g., '311', 'mta')
        date_str: optional date string to append to filename
    """
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Create filename
    if date_str:
        filename = f"data/{data_type}_{date_str}.csv"
    else:
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"data/{data_type}_{date_str}.csv"
    
    # Save to CSV
    df.to_csv(filename, index=False)
    logging.info(f"Saved {len(df)} records to {filename}")
    
    return filename