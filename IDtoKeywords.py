import json
import re
#from pprint import pprint
import pymongo

from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
from bson.json_util import loads
from sets import Set

def getkeywords(colName,keywords):
	teams=re.match('^(.+)\_(.+)\_.+\_.+', colName)
	json_data=open("keywords_brazil.py")
	key_teams=json.load(json_data)
	for i in range(1,3):
		if teams.group(i) in key_teams.keys():
			words = key_teams[teams.group(i)]
			for word in words:
				keywords.append(word.lower())
	#print type(keywords),len(keywords),keywords

def countkeywords():
	client = MongoClient()
	db =client.worldcup_structured
	colls=db.collection_names()
  
	for col in colls:
		if 'system.' in col or 'twitter_' in col:continue
       
		Cur=db[col].find({},{'id_str':1, 'text':1, 'created_at':1, '_id':0 })
		keylist=[]
		tweets_dict = {}
		getkeywords(col, keylist)    
		print "here len is",len(keylist)
		for cur in Cur:
			text=cur['text']            
			tweet=text.encode('utf-8').strip()
			tweet = tweet.lower()

			tweetRec = {}
			keywordsList = []
			for key in keylist:
				pattern = "[#@\"\'.]*"+key+"[#@\"\'.,?!:;]*"
				searchObj = re.search(pattern,tweet,flags=0)
				if searchObj:
					print tweet," -- ",key
					keywordsList.append(key)
			tweetRec["keywords"] = keywordsList
			#tweetRec["created_at"] = cur['created_at']
			tweets_dict['t_'+cur['id_str']] = tweetRec

		print tweets_dict	
def main():
	countkeywords()


if __name__=="__main__":main()
