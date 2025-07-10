import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

def validate_user_visits(users_df:pd.DataFrame,
                         logins_df:pd.DataFrame) -> None:
    """
    Validate that the number of visits in the last 30 days matches the login count
    """
    # get number of logins per user
    login_count = logins_df\
            .groupby('username')\
            .count()\
            .reset_index()\
            .rename(columns={'username':'email',
                             'login_timestamp':'login_count'})
    # merge with users
    users_login_count = users_df.merge(login_count, on='email', how='left').fillna({'login_count': 0})
    # get total cound and count of matches
    total_n = len(users_login_count)
    count_match = (users_login_count['website_visits_last_30_days'] == users_login_count['login_count']).sum()
    if total_n == count_match:
        logging.info("All visits match login count")
    elif count_match == 0:
        logging.error("No visits match login count")
    else:
        logging.warning(f"{round(100*total_n/count_match):0.2f}% match of visits to login count")
    return