import pymongo
from tweepy import StreamListener
import sys, time, json
from tweepy import auth
from tweepy import OAuthHandler
import smtplib
import tweepy
from HTMLParser import HTMLParser

# Define twitter authentication requirements
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
OAUTH_TOKEN = ''
OAUTH_TOKEN_SECRET = ''


# For testing purposes of mongodb
# client = pymongo.MongoClient("10.0.1.10", 27017)
# db = client.presidential
# coll = db.presidential

# cursor = coll.find()

# for item in cursor:
#     print(item['coordinates'])

class MyStreamListener(StreamListener):

    def __init__(self, api):
        self.api = api
        super(StreamListener, self).__init__()
        self.db = pymongo.MongoClient('10.0.1.10', 27017).presidential
        self.tweetcount = 0

    def on_data(self, data):
        decoded = json.loads(HTMLParser().unescape(data))
        try:
            tweet = decoded['text']
            if decoded['coordinates'] or decoded['geo']:
                self.tweetcount += 1
                print("Inserting tweet number {} into database".format(self.tweetcount))
                print(decoded['coordinates'])
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

def start_stream():
    while True:
        try:
            auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
            auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
            api = tweepy.API(auth)
            stream = tweepy.streaming.Stream(auth, MyStreamListener(api))
            stream.filter(track=q, async=True)
        except:
            continue


start_stream()

if __name__ == '__main__':
    start_stream()
