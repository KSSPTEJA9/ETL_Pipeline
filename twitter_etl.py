import pandas as pd
import tweepy
import json
from datetime import datetime
import s3fs


def run_twitter_etl():

    access_key="1684771411968077824-1GwqgNFE1s96jAU6cYvg7V32IpWgI4"
    access_secret="y6yMkPJwCPK4adxdh6VL6iKXIQEged6xojY2JpBCRiNwa"
    consumer_key="yH3452rrdQV1XXHlsUIz12RXK"
    consumer_secret="bD11qYdGwhCGrrrK7uFIeDmKQAy3X5a2tiZetHgasRkbDepiLQ"

    # Twitter authentication 
    auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_key,access_secret)

    # Creating an API object

    api =  tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                            count=200, include_rts= False, 
                            tweet_mode='extended')

    tweet_list=[]
    for tweet in tweets:
        text= tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text': text,
                        'favorite_count': tweet.favorite_count,
                        'retweet_count': tweet.retweet_count,
                        'created_at': tweet.created_at}
        tweet_list.append(refined_tweet)

    print(tweets)

    df = pd.DataFrame(tweet_list)
    df.to_csv("elonmusk_twitter_data.csv")

