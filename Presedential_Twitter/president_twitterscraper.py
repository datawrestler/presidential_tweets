import pymongo
from tweepy import StreamListener
import sys, time, json
from tweepy import auth
from tweepy import OAuthHandler
import smtplib
import tweepy
from HTMLParser import HTMLParser

# Define twitter authentication requirements
CONSUMER_KEY = 'jqQe3zzpuRhHY8Tyc1uF03reg'
CONSUMER_SECRET = 'cU6KVPX6Fs4fdl8HNNfeM5H51GyBq8GpO07aYMYWRnUoNiEeAy'
OAUTH_TOKEN = '2464948295-VEuAYV70fxxj7MbIQObktGOd3lyFrRD9awCKmA5'
OAUTH_TOKEN_SECRET = 'yD8r3Y3awNWMA6wywGCpUcp3JEAqm2mNqWqU8gewGkv9U'

db = pymongo.MongoClient("10.0.1.10", 27017)
coll = db['presidential']
coll.collection_names()

class MyStreamListener(StreamListener):

    def __init__(self, api):
        self.api = api
        super(StreamListener, self).__init__()
        self.db = pymongo.MongoClient('10.0.1.10', 27017).presidential
        self.tweetcount = 0

    def on_data(self, data):
      #  if self.tweetcount % 1000 == 0:
       #     try:
        #        server = smtplib.SMTP("smtp.mail.yahoo.com", 587)
         #       server.starttls()
         #       server.login("email", "password")
         #       server.sendmail('email_from', 'phone_number', 'Currently working on tweet number {}!'.format(self.tweetcount))
         #   except:
         #       pass
        decoded = json.loads(HTMLParser().unescape(data))
        try:
            tweet = decoded['text']
            if decoded['coordinates']:
                print(decoded['coordinates'])
                print(tweet)
                print(decoded)
                self.db.presidential.insert(json.loads(data))
            if decoded['geo']:
                print(decoded['geo'])
                print(tweet)
                print(decoded)
                self.db.presidential.insert(json.loads(data))
        except:
            pass

    def on_error(self, status_code):
        if status_code == 420:
            # return false in on_data disconnects the stream
            print('Disconnecting streaming listener due to {} error'.format(status_code))
            return True
        elif status_code == 401:
            print('Encountered 401 Error (Not Authorized)')
            return True
        elif status_code == 429:
            print("Ecnounterred 429 Error (Rate Limiting Exceeded")
            print(sys.stderr, "retrying in 15 minutes.....ZzzZ....")
            time.sleep(60*15 + 5)
            print("....ZzzzZ...Awake now and trying again.")
            return True
        elif status_code in (500, 502, 503, 504):
            print("Going to sleep due to error code {}".format(status_code))
            time.sleep(60*15 + 5)
            print("....ZZzzzzZZZ...Awake and trying again")
            return True

    def on_timeout(self):
        return True


q = ['Trump', 'trump', 'Donald Trump', '#trump', '#Trump', 'Rubio', 'Marco Rubio', 'rubio',
     'marco rubio', '#rubio', '#Rubio', 'Sanders', 'sanders', 'Bernie Sanders', 'bernie sanders',
     'Clinton', 'clinton', 'Hilary Clinton', 'hilary clinton', '#Clinton', '#clinton', 'Ted Cruz', 'ted cruz', 'cruz',
     'Cruz', '#cruz', '#Cruz', 'John Kasich', 'john kasich', 'kasich', '#kasich', '#Kasich',
     'Make America Great Again', 'make america great again', 'A Future To Believe In', 'a future to believe in',
     '#makeamericagreatagain', '#MakeAmericaGreatAgain', 'AFutureToBelieveIn', '#afuturetobelievein',
     'Reigniting the Promise of America', 'reigniting the promise of america', '#reignitingthepromiseofamerica',
     '#ReignitingThePromiseOfAmerica', 'Hilary for America', '#hilaryforamerica', '#HilaryforAmerica',
     'hilary for america', 'K for US', 'k for us', '#kforus', '#KforUS']


try:
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)
    sapi = tweepy.streaming.Stream(auth, MyStreamListener(api))
    sapi.filter(track=q)
except KeyboardInterrupt:
    print("Interupted by Keyboard")

if __name__ == '__main__':
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)
    sapi = tweepy.streaming.Stream(auth, MyStreamListener(api))
    sapi.filter(track=q) #set async to True so stream will run on new thread

