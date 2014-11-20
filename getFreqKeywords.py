import json
import re
from pprint import pprint
import pymongo

from pymongo import MongoClient
from bson.son import SON


def main():
	client = MongoClient()
	db =client.nba_stream_tweet_keyword
        colls=db.collection_names()
        db_store=client.nba_freq_kwds
        for col in colls:
		if 'system.' in col or 'twitter_' in col:continue
                #print col;
		res=db[col].aggregate([
			{"$unwind": "$keywords"},
			{"$group": {"_id":"$keywords", "count":{"$sum":1}}},
			{"$sort": SON([("count",-1), ("_id", -1)])}
		])
		#fout=open('Freq_of_Keywords', 'w')
		#json.dump(res['result'], fout, indent=4)
	        db_store[col].insert(res['result'])
		#pprint(res)
if __name__ =="__main__":main() 
