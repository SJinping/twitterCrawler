require 'json'
require 'mongo'
include Mongo

 mongo_client = MongoClient.new("localhost", 27017)
 coll = mongo_client['idea']['geo_tweets']
 #mongo_client.database_info.each { |info| puts info.inspect }

find_twitters = Proc.new{ |lang|
	w = File.open "#{lang}_data.csv","w"
	#w.write "text\n"
	incorrect = 0
	coll.find("lang" => lang).each do |data|
		begin
			text = data["text"]
			#coordinates =  data["coordinates"]["coordinates"]
			#lon,lat = coordinates
			#date = data["created_at"]
			w.write "#{text}\n"
		rescue
			incorrect +=1
		end

	end
	puts incorrect
	}

["zh"].each do |lang|
	find_twitters.call lang
end
