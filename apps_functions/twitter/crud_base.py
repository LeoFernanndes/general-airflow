import sys
sys.path.append('/home/ubuntu/environment/airflow/apps_functions/twitter')

import tweepy as tw
import mysql.connector
import json
import os
from datetime import datetime
from datetime import timedelta
import time
import pandas as pd
import twitter

from dotenv import load_dotenv
load_dotenv()


mydb = mysql.connector.connect(
    host = os.environ.get('MYSQL_TWITTER_HOST'),
    user = os.environ.get('MYSQL_TWITTER_USER'),
    port = os.environ.get('MYSQL_TWITTER_PORT'),
    password = os.environ.get('MYSQL_TWITTER_PASSWORD'),
    database = os.environ.get('MYSQL_TWITTER_DATABASE')
)

cursor = mydb.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS userlist (
        user_ids VARCHAR(100) PRIMARY KEY,
        last_query_date DATETIME
    );
""")

cursor.execute("""
    INSERT IGNORE INTO `userlist` (user_ids) 
    VALUES ('Biakicis'), ('marcelofreixo'), ('danielPMERJ');
""")
mydb.commit()


userlist = pd.read_sql("SELECT user_ids FROM userlist;", con=mydb)
mydb.close()

print(list(userlist.user_ids))




def get_userlist():
    mydb = twitter.mysql_rds_database_authentication()
    users_df = pd.read_sql("SELECT user_ids FROM userlist;", con=mydb)
    userlist = list(users_df.user_ids)
    mydb.close()
    return {'userlist': userlist}
    
print(get_userlist())
    
    