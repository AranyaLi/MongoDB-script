import pymongo;
#import string;
import re;

from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
from bson.json_util import loads
def main():
 
  fout=open("allids","w")
  client = MongoClient()
  db =client.twitter_jason
  #org_coll=db.TEST_2014626#read from
  #new_coll=db.NEW_TEST#write to
  colls=db.collection_names()
  value=[]
  for col in colls:
      #cur=db[col].find({},{'entities.user_mentions.id_str':1,'in_reply_to_user_id_str':1, 'user.id_str':1  })
      cur_mention=db[col].find({},{'entities.user_mentions.id_str':1, '_id':0})
      cur_replay=db[col].find({},{'in_reply_to_user_id_str':1, '_id':0})
      cur_user=db[col].find({},{'user.id_str':1, '_id':0 })
      #entry=[]
      for cm in cur_mention:
          #ntry= ast.literal_eval(json.dumps(c))['entities']['user_mentions']
          entry = dumps(cm)

          idlist= loads(entry)['entities']['user_mentions']
          for id in idlist:
               if id['id_str'] not in value:
                   value.append(id['id_str'])
                   fout.write(id['id_str']+'\n')
      for cr in cur_replay:
          entry = dumps(cr)
          id= loads(entry)['in_reply_to_user_id_str']
          if id not in value and id:
                   value.append(id)
                   fout.write(id+'\n')
              
      for cu in cur_user:
          entry = dumps(cu)
          #print entry
          id= loads(entry)['user']['id_str']
          if id not in value:
                   value.append(id)
                   fout.write(id+'\n')
              
  
      fout.close()

if __name__ =="__main__":main() 
