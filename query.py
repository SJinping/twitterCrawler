import pymongo
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
collection = client['idea']['geo_tweets']
cursor=collection.find({"lang":"en"})
#print("tweet_id\tuser_id\tdate\ttext")
for data in cursor:
	try:
		tweet_id = data["id"]
		user_id = data["user"]["id"]
		date = data["created_at"]
		text = data["text"]
		lon, lat = data["coordinates"]["coordinates"]

		print("%d\t%s\t%s\t%f\t%f" % (user_id,text,date,lon,lat))
	except:
		continue