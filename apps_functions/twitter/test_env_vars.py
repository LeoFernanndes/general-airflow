import sys
sys.path.append('/home/ubuntu/environment/airflow/apps_functions/twitter')
sys.path.append('/home/ubuntu/environment/modulo')

import os
from dotenv import load_dotenv
load_dotenv()

import testes
import twitter


testes.teste()
twitter.twitter_authentication()
twitter.mysql_rds_database_authentication()


userlist = ['Biakicis', 'marcelofreixo', 'danielPMERJ', 'opropriolavo', 'bolsonarosp', 'CarlaZambelli38', 'jairbolsonaro',] 
twitter.tweets_by_userlist(userlist)