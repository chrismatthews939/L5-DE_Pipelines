import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

def update_login_table(logins_df, conn):
    try:
        sql_str = """
        SELECT DISTINCT
        user_id
        , email
        FROM users
        order by user_id
        """
        key_lkp = pd.read_sql(sql_str, conn)
        logins_df_lkp = logins_df.merge(key_lkp, left_on='username', right_on='email', how='inner')
        logins_df_lkp = logins_df_lkp[['user_id', 'login_timestamp']]
        new_count = logins_df_lkp.to_sql('logins', conn, if_exists='append', index=False)
        logging.info(f"Inserted {new_count} new records into logins table")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        conn.rollback()
    finally:
        conn.commit()
    return


def update_users_table(users_df, conn):
    try:
        new_count = users_df.to_sql('users', conn, if_exists='append', index=False)
        logging.info(f"Inserted {new_count} new records into users table")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        conn.rollback()
    finally:
        conn.commit()
    return
    
def update_login_table(logins_df, conn):
    try:
        sql_str = """
        SELECT DISTINCT
        user_id
        , email
        FROM users
        order by user_id
        """
        key_lkp = pd.read_sql(sql_str, conn)
        logins_df_lkp = logins_df.merge(key_lkp, left_on='username', right_on='email', how='inner')
        logins_df_lkp = logins_df_lkp[['user_id', 'login_timestamp']]
        new_count = logins_df_lkp.to_sql('logins', conn, if_exists='append', index=False)
        logging.info(f"Inserted {new_count} new records into logins table")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        conn.rollback()
    finally:
        conn.commit()
    return