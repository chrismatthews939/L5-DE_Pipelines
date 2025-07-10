import logging
import pandas as pd
import hashlib
import datetime
import re

# fix column names
def tidy_columns(df, mapping=None):
    df.columns = [x.lower().strip().replace(' ', '_') for x in df.columns]
    if mapping:
        df = df.rename(columns=mapping)
    return df

# fix DOB
def clean_dob(value, lim_year=25):
    if pd.isna(value):
        return None
    else:
        day, month, year = map(int, value.strip().split('/'))
        if year >= lim_year:
            year += 1900
        else:
            year += 2000
        return datetime(year, month, day)


def infer_dob(date, age):
    formats = ["%d/%m/%y", "%y-%m-%d", "%m/%d/%y"]
    for fmt in formats:
        try:
            dob = datetime.strptime(date.strip(), fmt)
            dob = dob.replace(year=datetime.now().year - int(age))
            return dob.date()
        except:
            continue
    return None


def hash_password(pw, encoding='utf-8'):
    if pd.isna(pw):
        return None
    else:
        return hashlib.sha256(pw.encode(encoding)).hexdigest()


def clean_gender(value, mapping=None):
    if pd.isna(value):
        return None
    elif mapping is None:
        return value
    else:
        try:
            new_value = mapping[value]
            return new_value
        except KeyError as e:
            logging.warning(f"Unknown gender value: {value}")
            return None
        except Exception as e:
            logging.error(f"Error cleaning gender value: {value}, {e}")
        return None


def clean_number(value):
    if pd.isna(value):
        return None
    else:
        number = re.sub(r"[^\d]", '', value)
        if number.startswith('0'):
            number = number[1:]
        return number


def clean_salary(value, period=1):
    if pd.isna(value):
        return None
    else:
        salary = round(int(re.sub(r"[^\d]", '', value))/100, 2)* period
        if salary < 0:
            logging.warning(f"Negative salary found: {value}")
        return salary


def clean_column(value):
    exclusions = ['BLANK', 'NA', 'NONE', '-', '{NULL}', 'VIDE', '']
    if isinstance(value, str) and value.strip().upper() in exclusions:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    else:
        return value


