require 'tweetstream'
require 'json'
require 'mongo'
require 'time'

include Mongo

TweetStream.configure do |config|
  config.consumer_key       = ''
  config.consumer_secret    = ''
  config.oauth_token        = ''
  config.oauth_token_secret = ''
  config.auth_method        = :oauth
end


keyFilter = %w(id text lang media created_at user coordinates place entities)
userFilter = %w(id name screen_name lang location description statuses_count)
#puts keyFilter

Mongo::Logger.logger.level = Logger::WARN

# mongo_client = MongoClient.new("localhost", 27017)
  mongo_client = Mongo::Client.new([ '127.0.0.1:27017' ], :database => 'idea')
  coll = mongo_client[:geo_tweets_2]
  # coll = mongo_client['geo_tweets_2']

while true do
# begin

  dataList = []
  TweetStream::Client.new.locations(-180,-90,180,90) do |status|

  # The status object is a special Hash with
  # method access to its keys.
  #da(status.to_h)
    data = status.to_h
    data.select!{|key,value| keyFilter.include? key.to_s}
    data[:user].select!{|key,value| userFilter.include? key.to_s}
    data[:created_at] = Time.parse data[:created_at]

  #puts JSON.pretty_generate(data)
    dataList.push(data)
    #puts data
    # puts dataList.length
    if dataList.length > 100#10000
  	 # bulk = coll.initialize_ordered_bulk_op
     # bulk = coll('geo_tweets_2').initializeOrderedBulkOp()
     puts "超過100條記錄啦！"
     # puts bulk

  	 dataList.each do |data|
        coll.bulk_write([{ :insert_one => data}], :ordered => true)


  		  # bulk.insert(data)
  	 end
  	 # p bulk.execute
     puts "Insert"
     # count = 0
     # view = coll.find
     # view.each do |document|
     #      count += 1
     #      # p document
     #    end
     # p count
  	 dataList = []
    end

  
  end
rescue
  puts "Hey"
  sleep 10
end
end
#puts JSON.pretty_generate(x)
#rtc = retweet count id, text, lang, hashtags(emtity), media, created, screenname STC = "statuses_count,

