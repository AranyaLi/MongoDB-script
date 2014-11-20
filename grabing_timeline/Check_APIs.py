import tweepy
import json
from src.auth_keys import keys


def get_api(a):

	auth = tweepy.OAuthHandler(a['consumer_key'], a['consumer_secret'])
	auth.set_access_token(a['access_token'], a['access_token_secret'])
	api = tweepy.API(auth)
	return api

fout = open("./API_Status.txt", "w")
fout.write("There are %d APIs in use\n" % len(keys))

for key_id, k in keys.items():
	try:
		print "Checking API %s" % key_id
		api 	= get_api(k)
		state 	= api.rate_limit_status()
		fout.write("\n %s " % key_id)
		res 		= state['resources']
		status 		= res['statuses']
		timeline 	= status['/statuses/user_timeline']
		if timeline['limit'] < 180:
			print "Less than 180\n"
		# fout.write(timeline)
		# temp = json.dumps(state)
		json.dump(timeline,fout,sort_keys = True, indent = 4)
	except tweepy.TweepError as e:
		fout.write("%s, %s" % key_id, e)
fout.close()

