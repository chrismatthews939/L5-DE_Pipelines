import pandas as pd
from datetime import datetime
import re
import hashlib

import logging
from pipeline.data_utils import (
    tidy_columns, clean_column, clean_gender, clean_number, clean_salary, hash_password, infer_dob
)

logging.basicConfig(level=logging.INFO)

def add_education_column(df, mapping):
    if mapping is None:
        logging.warning("No mapping provided for education column.")
    elif 'education' in df.columns:
        logging.info(f"Add RFQ column")
        df['rqf'] = df['education'].apply(lambda x: mapping.get(str(x), None))
    elif 'rqf' in df.columns:
        df['education'] = df['rqf'].apply(lambda x: mapping.get(str(x), None))
    else:
        logging.warning("No education or RFQ column found in the DataFrame.")
    return df


def transform_users(df, country_code,
                    column_mapping=None,
                    gender_mapping=None,
                    education_mapping=None,
                    payment_period=1,
                    int_dial_code='44',
                    currency='GBP'):
    logging.info("Transforming user data...")
    logging.info(f"Tidying columns")
    df = tidy_columns(df, column_mapping)
    df['gender'] = df['gender'].astype(str)
    logging.info(f"Cleaning DOB column")
    df['dob'] = df.apply(lambda row: infer_dob(row['dob'], row['age_last_birthday']), axis=1)
    logging.info(f"Hashing password")
    df['password'] = df['password'].apply(hash_password)
    logging.info(f"Cleaning string columns")
    for col in df.columns:
        if col not in ['password', 'dob']:
            df[col] = df[col].apply(clean_column)
    logging.info(f"Cleaning gender column")
    df['gender'] = df['gender'].apply(lambda row: clean_gender(row, gender_mapping))
    logging.info("Cleaning number columns")
    for col in ['phone', 'mobile']:
        df[col] = df[col].apply(clean_number)
    logging.info(f"Checking education column")
    df = add_education_column(df, education_mapping)
    logging.info(f"Cleaning salary column")
    df['salary'] = df['salary'].apply(lambda x: clean_salary(x, payment_period))
    logging.info("Setting International Dialing Code")
    df['dial_code'] = int_dial_code
    logging.info(f"Setting currency")
    df['currency'] = currency
    logging.info(f"Adding country code")
    df['country_code'] = country_code
    return df


