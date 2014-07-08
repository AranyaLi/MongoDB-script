import json
import re
from pprint import pprint
import pymongo

from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
from bson.json_util import loads
from sets import Set
#from text.blob import TextBlob
#keydict={}
import pprint
def getkeywords(colName, keydict):
    teams=re.match('^(.+)\_(.+)\_(.+)', colName)
    team1=teams.group(1)
    team2=teams.group(2)
    #print team1, team2
    json_data=open("keywords.py")
    keys=json.load(json_data)
    #pprint (keys)
    for key in keys[team1]:
       if key not in keydict.keys():
           keydict[key]=0;
    
    for key in keys[team2]:
       if key not in keydict.keys():
           keydict[key]=0;
    # print len(keydict.keys())

def countkeywords():
  client = MongoClient()
  db =client.twitter_jason
  colls=db.collection_names()
  
  for col in colls:
       if 'system.' in col or 'twitter_' in col:continue
       
       Cur=db[col].find({'lang':'en'},{'entities.hashtags':1,'id_str':1,'user.id_str':1, 'text':1, '_id':0 })
       for cur in Cur:
            keydict={}
            getkeywords(col, keydict)    
            text=cur['text']
            userid=cur['user']['id_str']
            hashtag=cur['entities']['hashtags']
            twitterid=cur['id_str']
            
	    words=text.encode('utf-8').strip()
            words=re.split(' ', words)
            lowerText=[]
	    for w in words:
                w=re.sub(r'[!?"(),:...]', '', w)
                lowerText.append(w.lower())
                      
            print lowerText
            lowerKeys=eval(repr(keydict).lower())
            #print lowerKeys
            useful=0
            nouse=0
            for key in lowerKeys:
                # print key
                #print key, lowerKeys
                key=key.encode('utf-8')
                quotkey=key+"'"
                dquotkey='"'+key+'"'
                dotkey=key+'.'
                hashkey='#'+key
                atkey='@'+key
                if key in lowerText or quotkey in lowerText or dotkey in lowerText or hashkey in lowerText or atkey in lowerText or dquotkey in lowerText:
                  print key
                keyset=set(re.split(r'\s', key))
                if keyset.issubset(set(lowerText)):
                     print keyset
                else:
		   continue
                '''if 'http' in word:continue
                else:
                    word=re.sub(r'[."@#(),:]', '', word)
                # print word
                #if word.lower() in lowerDict:
                     # keydict[word]+=1'''        
            if useful==0:
                 nouse=1
                 #print text
                 #pprint.pprint (keydict.keys())
            #print nouse
def main():
   #getkeywords("Toronto_Milwaukee_2014414")
   countkeywords()


if __name__=="__main__":main()
