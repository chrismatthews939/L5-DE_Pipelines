#!/usr/bin/env python
# coding: utf-8

import sqlite3
import logging
import json
import subprocess
from pipeline.load_data import load_user_data, load_login_data
from pipeline.transform import transform_users
from pipeline.database import update_login_table, update_users_table
from pipeline.load_data import load_user_data, load_login_data
from tests.validation import validate_user_visits
from pipeline.data_utils import tidy_columns
from pipeline.config_loader import load_json_config

logging.basicConfig(level=logging.INFO)

def load_json_config(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    uk_config = load_json_config('config/mappings_uk.json')
    fr_config = load_json_config('config/mappings_fr.json')
    usa_config = load_json_config('config/mappings_usa.json')
    sc_config = load_json_config('config/mappings_sc.json')

    #UK 
    users_uk = load_user_data('data/UK User Data.csv',encoding='latin1')
    users_uk = transform_users(users_uk, 
                            country_code='UK',
                            education_mapping=uk_config['education_mapping'],
                            currency='GBP')
    logins_uk = load_login_data('data/UK-User-LoginTS.csv', 'Europe/London')
    validate_user_visits(users_uk, logins_uk)

    #France
    users_fr = load_user_data('data/FR User Data.csv')
    users_fr = transform_users(users_fr,
                            country_code='FR',
                            column_mapping=fr_config['column_mapping'],
                            gender_mapping=fr_config['gender_mapping'],
                            education_mapping=fr_config['education_mapping'],
                            payment_period=12,
                            currency='EUR')
    logins_fr = load_login_data('data/FR-User-LoginTS.csv', 'Europe/Paris')
    validate_user_visits(users_fr, logins_fr)

    #USA
    users_usa = load_user_data('data/USA User Data.csv')
    users_usa = transform_users(users_usa, country_code='USA',
                                column_mapping=usa_config['column_mapping'],
                            gender_mapping=usa_config['gender_mapping'],
                            education_mapping=usa_config['education_mapping'],
                            currency='USD')
    logins_usa = load_login_data('data/USA-User-LoginTS.csv', 'US/Eastern')
    validate_user_visits(users_usa, logins_usa)

    #Scotland
    users_sc = load_user_data('data/SC User Data.csv')
    users_sc = transform_users(users_sc, country_code='SC',
                                column_mapping=sc_config['column_mapping'],
                            gender_mapping=sc_config['gender_mapping'],
                            education_mapping=sc_config['education_mapping'],
                            currency='EUR')
    logins_sc = load_login_data('data/SC-User-LoginTS.csv', 'Europe/London')
    validate_user_visits(users_sc, logins_sc)


    subprocess.run("sqlite3 customer.db < sql/create_database.sql", shell=True, check=True)
    conn = sqlite3.connect("customers.db")

    update_users_table(users_uk, conn)
    update_users_table(users_fr, conn)
    update_users_table(users_usa, conn)
    update_users_table(users_sc, conn)

    update_login_table(logins_uk, conn)
    update_login_table(logins_fr, conn)
    update_login_table(logins_usa, conn)
    update_login_table(logins_sc, conn)

    conn.close()

if __name__ == "__main__":
    main()