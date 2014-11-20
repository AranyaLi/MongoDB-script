import pymongo;
#import string;
import re;

from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
from bson.json_util import loads
def main():
 
  #fout=open("allids","w")
  client = MongoClient()
  db =client.nba_main
  db2=client.nba_stream_id
  #org_coll=db.TEST_2014626#read from
  #new_coll=db.NEW_TEST#write to
  colls=db.collection_names()
  value=[]
  for col in colls:
      if 'system.' in col or 'twitter_' in col:continue
      cur=db[col].find({},{'entities.user_mentions.id_str':1,'in_reply_to_user_id_str':1, 'user.id_str':1, 'id_str':1, '_id':0  })
      for tweet in cur:
          entry=dumps(tweet)
          tweetid=loads(entry)['id_str']
          mentionid= loads(entry)['entities']['user_mentions']
          for id in mentionid:
               if id['id_str'] not in value:
                   value.append(id['id_str'])
                   db2[col].insert({'UserID':id['id_str'], 'TweetId':tweetid })
          replyid= loads(entry)['in_reply_to_user_id_str']
          if replyid not in value and replyid:
                   value.append(replyid)
                   db2[col].insert({'UserID':replyid,'TweetId':tweetid })
          userid= loads(entry)['user']['id_str']
          if userid not in value:
                   value.append(userid)
                   db2[col].insert({'UserID':userid,'TweetId':tweetid})
          

if __name__ =="__main__":main() 
