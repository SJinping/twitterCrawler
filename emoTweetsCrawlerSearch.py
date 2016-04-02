import sys
sys.path.append("/home/pan/Idealab/codes/twitterKeys")
import keys
import time
from twython import Twython, TwythonRateLimitError
from twython import TwythonStreamer
from pymongo import MongoClient
from datetime import datetime
from nltk.tokenize import TweetTokenizer

from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    AdaptiveETA, FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer

'''
This code is used to crawl tweets with specified hashtags. Specifically, it is used to collect emotion tweets.
It uses tweet search API instead of stream API.
For more information about search API, please check:
https://twython.readthedocs.org/en/latest/api.html
https://dev.twitter.com/rest/reference/get/search/tweets
'''

'''
db names: emo_twitter
collection names: tweets_anger, tweets_joy, tweets_sadness, tweets_depression
'''

tknzr = TweetTokenizer()
emos = ['#depressed', '#desperate']
emo_name = 'depression'

#>>>>>>>>>> Set since_id and max_id if needed >>>>>>>>>>
# If you don't want to set max_id or since_id, leave them None.
# since_id = 705352350609731584
since_id = None
# max_id = 708228537421946880 - 1
_max_id = None # global var. Its value may change
#<<<<<<<<<< Set since_id and max_id if needed >>>>>>>>>>

class TweetSearch(Twython):

	_last_tweet_id = _max_id
	_is_first_req = True
	_count = 0

	def tweet_filter(self,data):

		keys = "id text lang media user coordinates place entities".split()
		user_keys = "id name screen_name lang location description statuses_count".split()
		tweet = {key: value for key, value in data.items() if key in keys }
		tweet["user"] = {key: value for key, value in data["user"].items() if key in user_keys }
		tweet["created_at"] = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
		return tweet


	def __init__(self, consumer_key, consumer_secret, oauth_token, oauth_token_secret):
		super(TweetSearch, self).__init__(consumer_key, consumer_secret, oauth_token, oauth_token_secret)
		self.tweets_buffer = []
		self.collection = MongoClient("localhost", 27017)["emo_twitter"]["tweets_" + emo_name]

	def proceed_tweets(self, query, count, lang, max_id, sinceid, include_entities = 'true'):

		global _max_id # if you want to access or modify a global var in a function, you must specify the global keyword before use the var

		try:
			if self._is_first_req:
				tweets = self.search(q = query, count = count, lang = lang, include_entities = include_entities)
				self._is_first_req = False
			else:
				tweets = self.search(q = query, count = count, lang = lang, max_id = max_id, include_entities = include_entities)
		except TwythonRateLimitError:
			print 'Exceed the rate limits. Take a break :)'
			self.sleep()
		else:
			self._last_tweet_id = _max_id
			for tweet in tweets['statuses']:
				self._last_tweet_id = int(tweet["id_str"]) - 1  # Please check Twitter's developer website to know why -1 is added.
				tweet = self.tweet_filter(tweet)
				self.tweets_buffer.append(tweet)
			_max_id = self._last_tweet_id
			self.collection.insert(self.tweets_buffer)
			print("{} {} tweets have been inserted".format(len(self.tweets_buffer), emo_name))
			self.tweets_buffer = []

	def sleep(self):
		pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=930).start()
		for i in range(930):
			time.sleep(1)
			pbar.update(i+1)
		pbar.finish()

	def set_last_id(self, lastid = _max_id):
		self._last_tweet_id = lastid


def hashtag_query(hashtags):
	if len(hashtags) == 1:
		return hashtags[0]
	result = [tag for tag in hashtags]
	result = ' OR '.join(result)
	return result



consumer_key       = keys.CONSUMERKEY()
consumer_secret    = keys.CONSUMERSECRET()
oauth_token        = keys.OAUTHTOKEN()
oauth_token_secret = keys.OAUTHTOKENSECRET()


is_first_req = True
query = hashtag_query(emos)
last_tweet_id = _max_id
api = TweetSearch(consumer_key, consumer_secret, oauth_token, oauth_token_secret)
api.set_last_id(_max_id)
while True:
	try:
		api.proceed_tweets(query, 100, 'en', _max_id, since_id)
	except Exception, e:
		print e
