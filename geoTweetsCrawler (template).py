from twython import TwythonStreamer
from pymongo import MongoClient
from datetime import datetime

# This code crwals tweets and stores them in MongoDB.
# If you don't want to use MongoDB to store the data, you can just print them on the screen or print them in a text file.

class TweetStreammer(TwythonStreamer):

	def tweet_filter(self,data):

		keys = "id text lang media user coordinates place entities".split()
		user_keys = "id name screen_name lang location description statuses_count".split()
		tweet = {key: value for key, value in data.items() if key in keys }
		tweet["user"] = {key: value for key, value in data["user"].items() if key in user_keys }
		tweet["created_at"] = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
		return tweet


	def __init__(self, consumer_key, consumer_secret, oauth_token, oauth_token_secret):
		super(TweetStreammer, self).__init__(consumer_key, consumer_secret, oauth_token, oauth_token_secret)
		self.tweets_buffer = []
		self.bulk_size = 1000
		self.collection = MongoClient("localhost", 27017)["twitter"]["geo_tweets"] # connect to MongoDB

	def on_success(self, data):

		if "user" not in data:  #Skip the non-tweet request result
			return
		#print(data)
		tweet = self.tweet_filter(data)

		self.tweets_buffer.append(tweet)

		#print(tweet["coordinates"])
	
		if len(self.tweets_buffer) >= self.bulk_size:
			self.collection.insert(self.tweets_buffer) # Insert into MongoDB
			print("{} tweets have been inserted".format(len(self.tweets_buffer)))
			self.tweets_buffer = []


	def on_error(self, status_code, data):
		print(status_code)

# You must apply keys before you crawl tweets
# To apply for the keys, you can go to: https://apps.twitter.com/
consumer_key       = 'Your consumer key'
consumer_secret    = 'Your consumer secret'
oauth_token        = 'Your oauth token'
oauth_token_secret = 'Your oauth token secret'




geo_stream = TweetStreammer(consumer_key, consumer_secret, oauth_token, oauth_token_secret)
while True:
	try:
		geo_stream.statuses.filter(locations = [-180,-90,180,90])  # Here indicate location of the tweet. It is also can be indicated null
	except:
		pass
