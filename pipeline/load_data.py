import logging
import pandas as pd
from pipeline.data_utils import tidy_columns

def load_user_data(filepath, encoding='utf-8'):
    logging.info(f"Loading data from {filepath}")
    df = pd.read_csv(filepath, encoding=encoding)
    return df

def load_login_data(filepath, timezone):
    logging.info(f"Loading login data from {filepath}")
    df = pd.read_csv(filepath)
    df.columns = ['login_id', 'username', 'login_timestamp']
    logging.info(f"Tidying columns")
    tidy_columns(df)
    df.drop(columns=['login_id'], inplace=True)
    # convert timestamp to datetime
    df['login_timestamp'] = pd.to_datetime(df['login_timestamp'], unit='s', utc=False)
    df['login_timestamp'] = df['login_timestamp'].dt.tz_localize(timezone).dt.tz_convert('UTC')
    return df