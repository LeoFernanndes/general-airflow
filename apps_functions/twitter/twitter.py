import tweepy as tw
import mysql.connector
import json
import os
from datetime import datetime
from datetime import timedelta
import time
import pandas as pd

from dotenv import load_dotenv
load_dotenv()


def twitter_authentication():
    
  auth = tw.OAuthHandler(consumer_key = os.environ.get('TWITTER_API_CONSUMER_KEY'),
                         consumer_secret = os.environ.get('TWITTER_API_CONSUMER_SECRET'))
  
  auth.set_access_token(os.environ.get('TWITTER_API_ACCESS_TOKEN'),
                        os.environ.get('TWITTER_API_ACCESS_SECRET'))
  
  api = tw.API(auth, wait_on_rate_limit=True)
  
  print('Autenticação no twitter')
  return api
  
  
def mysql_rds_database_authentication():
  
  mydb = mysql.connector.connect(
    host = os.environ.get('MYSQL_TWITTER_HOST'),
    user = os.environ.get('MYSQL_TWITTER_USER'),
    port = os.environ.get('MYSQL_TWITTER_PORT'),
    password = os.environ.get('MYSQL_TWITTER_PASSWORD'),
    database = os.environ.get('MYSQL_TWITTER_DATABASE')
  )

  print('Autenticação no banco de dados twitter_data')
  return mydb


def mysql_rds_django_authentication():
  
  mydb = mysql.connector.connect(
    host = os.environ.get('MYSQL_TWITTER_HOST'),
    user = os.environ.get('MYSQL_TWITTER_USER'),
    port = os.environ.get('MYSQL_TWITTER_PORT'),
    password = os.environ.get('MYSQL_TWITTER_PASSWORD'),
    database = 'twitter-monitor'
  )

  print('Autenticação no banco de dados django models arroba')
  return mydb



def tweets_by_userlist(**context):
  
  inicio = datetime.now()
  
  api = twitter_authentication()
  
  id, name, arroba, retweets, likes, text, date, location, hashtags, links, language, search = [], [], [], [], [], [], [], [], [], [], [], []
  
  count = 0
  
  userlist = context['task_instance'].xcom_pull(task_ids='get_userlist')['userlist']
  
  
  for userID in userlist:
    
    mydb = mysql_rds_database_authentication()
    newest_date = pd.read_sql(f"SELECT date FROM tweets where arroba = '{userID}' order by date desc limit 1;", con=mydb).date[0]
    mydb.close()
    
    tweets = api.user_timeline(screen_name=userID, 
                                # 200 is the maximum allowed count
                                count=1,
                                include_rts = True,
                                # Necessary to keep full_text 
                                # otherwise only the first 140 words are extracted
                                tweet_mode = 'extended'
                                )
  
    oldest_id = tweets[-1].id
  
  
    while len(tweets) > 0:
      
      count += 1
      if count % 50 == 0:
        time.sleep(10)
  
      mydb = mysql_rds_database_authentication()
      cursor = mydb.cursor()
  
      tweets = api.user_timeline(screen_name=userID, 
                                # 200 is the maximum allowed count
                                count=200,
                                include_rts = True,
                                # Necessary to keep full_text 
                                # otherwise only the first 140 words are extracted
                                max_id = int(oldest_id) - 1,
                                tweet_mode = 'extended',
                                )
  
      for tweet in tweets:
        if hasattr(tweet, "quoted_status"):
          full_text = "QT {} \n QUOTED: {}".format(tweet.full_text, tweet.quoted_status.full_text)    
        if hasattr(tweet, "retweeted_status") is False:
          full_text = "{}".format(tweet.full_text)
        if hasattr(tweet, "retweeted_status") is True:
          full_text = "RT @{}: {}".format(tweet.retweeted_status.user.screen_name, tweet.retweeted_status.full_text)
  
        id.append(tweet.id_str), name.append(tweet.user.name), arroba.append(tweet.user.screen_name),
        retweets.append(tweet.retweet_count), likes.append(tweet.favorite_count), text.append(full_text),
        date.append(tweet.created_at - timedelta(hours=3)), location.append(tweet.user.location),
        hashtags.append(str(tweet.entities.get("hashtags"))), links.append(str(tweet.entities.get("urls"))),
        language.append(tweet.lang), search.append(userID)
  
       
      print(len(id))
  
      if len(tweets) == 0:
        continue
      
      oldest_id = tweets[-1].id
  
      if date[-1] < newest_date:
        break
  
  tweets_df = pd.DataFrame({
      'id': id,
      'name': name,
      'arroba': arroba,
      'retweets': retweets,
      'likes': likes,
      'text': text,
      'date': date,
      'location': location,
      'hashtags': hashtags,
      'links': links,
      'language': language,
      'search': search
  })
  
  final = datetime.now()
  print(final - inicio)
  print(tweets_df.shape[0])
  
  # return tweets_df
  
  # persist data to database
  inicio = datetime.now()
  
  bulk = []
  for i in range(tweets_df.shape[0]):
  
    linha = []
  
    linha.append(tweets_df.loc[i]['id'])
    linha.append(tweets_df.loc[i]['name'])
    linha.append(tweets_df.loc[i]['arroba'])
    linha.append(tweets_df.loc[i]['retweets'])
    linha.append(tweets_df.loc[i]['likes'])
    linha.append(tweets_df.loc[i]['text'])
    linha.append(tweets_df.loc[i]['date'])
    linha.append(tweets_df.loc[i]['location'])
    linha.append(tweets_df.loc[i]['hashtags'])
    linha.append(tweets_df.loc[i]['links'])
    linha.append(tweets_df.loc[i]['language'])
    linha.append(tweets_df.loc[i]['search'])
  
    bulk.append(tuple(linha))
  
  final = datetime.now()
  print(final - inicio)

  
  
  inicio = datetime.now()
  
  sql =  f"""
          INSERT IGNORE INTO `tweets` (id, name, arroba, retweets, likes, text, date, location, hashtags, links, language, search)
          VALUES {str(bulk)[1:-1]};
          """
        
  mydb = mysql_rds_database_authentication()
  cursor = mydb.cursor()
  cursor.execute(sql)
  
  mydb.commit()
  mydb.close()
  
  final = datetime.now()
  print(final - inicio)
  print(f'{tweets_df.shape[0]} devidamente inseridas no banco')
 
  
  
def get_userlist():
    mydb = mysql_rds_django_authentication()
    users_df = pd.read_sql("SELECT arroba FROM gestao_usuarios_arrobamodel;", con=mydb)
    userlist = list(users_df.arroba)
    mydb.close()
    return {'userlist': userlist}