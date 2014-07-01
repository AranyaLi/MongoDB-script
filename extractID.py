import pymongo;
import string;

from pymongo import MongoClient

def main():

  client = MongoClient()
  db =client.twitter_jason
  #org_coll=db.TEST_2014626#read from
  #new_coll=db.NEW_TEST#write to
  colls=db.collection_names()
  for col in colls:
      print col
      cur=col.find({},{'entities.user_mentions.id_str':1,'in_reply_to_user_id_str':1, 'user.id_str':1 })
      for c in cur:
        print c

  #cursor1=org_coll.find({}, {'text':1, '_id':1})
  #for cur in cursor1:
     #new_coll.insert(cur)
     
 
  #print collection.find_one()
  #cursor= collection.find({}, {'text':1,'id':1,'coordinates':1, 'geo':1   })
  #cursor= collection.find({}, {'entities.media.id_str':1  })
  
  #for doc in cursor:
   #  print doc



if __name__ =="__main__":main() 
