# _*_ coding: utf-8 _*_
import sys
import time
from pymongo import MongoClient
from nltk.tokenize import TweetTokenizer


from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    AdaptiveETA, FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer

class TweetsQuery:
	"""docstring for TweetsQuery"""

	_collection = MongoClient()
	_querystr = ""
	_ip = ""
	_port = 0
	_db = ""
	_text = []
	_queryCount = 0
	_tagList = []

	# def __init__(self, arg):
	# 	super(TweetsQuery, self).__init__()
	# 	self.arg = arg
	def initDB(self, ip = 'localhost', port = 27017, db = 'twitter', collec = 'geo_tweets'):
		self._ip = ip
		self._port = port
		self._db = db
		client = MongoClient(self._ip, self._port)
		self._collection = client[self._db][collec]

	def tweetsCount(self):
		return self._collection.count()

	def setQuery(self, query = {'lang':'en'}):
		self._querystr = query

	def getQuery(self):
		return self._querystr

	def setQueryCount(self, num):
		self._queryCount = num

	def getQueryCount(self):
		return self._queryCount

	def setTagList(self, tags): # the tags in the list must contain "#"
		self._tagList = tags

	def getTagList(self):
		return self._tagList

	# get text tweets list.
	def getText(self):
		return self._text

	def getTagTweets(self):
		if len(self._tagList) <= 0:
			print "No tags provided! Please provide tag(s) first!"
			return

		self.getTweets()

	def getTweets(self):
		if self._queryCount <= 0:
			pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval = self._collection.count()).start()
		else:
			pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval = self._queryCount).start()

		print "Begin to collect tweets..."
		index = 0
		self._text = []
		
		if len(self._tagList) <= 0:
			if self._queryCount > 0:
				for tweet in self._collection.find(self._querystr):
					try:
						text = tweet['text']
						self._text.append(str(text))
						# time.sleep(0.01)
						pbar.update(index+1)
						index += 1

						if index >= self._queryCount:
							break

					except Exception, e:
						continue
					else:
						pass
				pbar.finish()
			else:
				for tweet in self._collection.find(self._querystr):
					try:
						text = tweet['text']
						self._text.append(text)
						pbar.update(index+1)
						index += 1
					except Exception, e:
						continue
					else:
						pass

		# Get tagged tweets
		else:
			tknzr = TweetTokenizer()
			if self._queryCount > 0:
				for tweet in self._collection.find(self._querystr):
					try:
						text = tweet['text']
						tokens = set(tknzr.tokenize(text))
						if len(tokens & set(self._tagList)) > 0: # If the tweet contains any tag that listed in the list
							self._text.append(str(text))
							pbar.update(index+1)
							index += 1
						
						if index >= self._queryCount:
							break

					except Exception, e:
						continue
					else:
						pass
				pbar.finish()
			else:
				for tweet in self._collection.find(self._querystr):
					try:
						text = tweet['text']
						tokens = set(tknzr.tokenize(text))
						if len(tokens & set(self._tagList)) > 0: # If the tweet contains any tag that listed in the list
							self._text.append(str(text))
							pbar.update(index+1)
							index += 1
					except Exception, e:
						continue
					else:
						pass
		

		print "Finishing collecting tweets!"
		print str(len(self._text)) + ' tweets have been queried.'

	def writeTweets2file(self, filePath):
		if len(filePath) <= 0:
			print "No tweets are collected!"
			return

		tweet_list = self.getText()

		pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval = len(tweet_list)).start()
		print "Begin to write tweets into file..."

		index = 0
		filewrite = open(filePath, 'w')
		for tweet in tweet_list:
			text = tweet.replace('\n', ' ') # 替換掉所有換行符
			text = text.replace(';', '.') # 所有分號替換成句號
			text = text.replace('\t', ' ') # 替換掉所有tab
			filewrite.write(text)
			filewrite.write('\t' + 'surprise' + '\t' + 'sadness') # 每行後面添加一個制表符，為了符合JAVA的simpleClassifier代碼
			filewrite.write('\r\n')
			pbar.update(index+1)
			index += 1

		pbar.finish()
		filewrite.close()
		print "Finishing writing tweets!"
		print str(len(self._text)) + ' tweets have been written into the file: ' + filePath

'''
以下代碼用完需要及時註釋掉
'''
# query = {'lang':'en'}
# crawler = TweetsQuery()
# crawler.initDB()
# crawler.setQuery(query)
# crawler.setQueryCount(30000)
# crawler.getTweets()
# crawler.writeTweets2file("/home/pan/Dropbox/UserTweets")
# tweet_list = crawler.getText()
'''
以上代碼用完需要及時註釋掉
'''
