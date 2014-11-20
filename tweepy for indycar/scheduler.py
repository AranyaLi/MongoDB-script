import time
import json
import logging

from datetime import datetime
from datetime import timedelta
from dateutil import parser
from apscheduler.scheduler import Scheduler
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from streaming_data import CustomStreamListener
from utility.auth_keys import keys
#from utility.nba_schedule_cst import schedule
from utility.schedule import schedule
from utility.keywordsIndycar import keywords as kws

#from utility.keywords import keywords as kws

def get_keywords():
    keywords = []
#add the keywords of both home and away team to keywords[]
    for k in kws.keys():
        #if home in k:
        #   keywords.extend(kws.get(k))
        #if away in k:
            keywords.extend(kws.get(k))
    #print kws.keys()
    #print keywords
    return keywords


def get_auth_account():
	# get an auth key that is not in use
    for key_id, token in keys.items():			#// what is the use of token??
        if key_id not in auth_keys_in_use.values():
            return key_id
    return None


def connect_streaming_api(keywords, game_datetime, game_name, job_count):
    '''
    Job thread
    '''

    # get a auth account to use
    auth_key = get_auth_account()
    auth_keys_in_use[str(job_count)] = auth_key

    l = CustomStreamListener(auth_key, game_datetime, game_name)
    auth = OAuthHandler(
        keys[auth_key]['consumer_key'], keys[auth_key]['consumer_secret'])
    auth.set_access_token(
        keys[auth_key]['access_token'], keys[auth_key]['access_token_secret'])

    stream = Stream(auth, l)
    stop_time = game_datetime + timedelta(hours=5)	#get related tweets from game_datetime to 5hours after;  

    while True:
        if stop_time < datetime.now():
            break

        try:
            stream.filter(track=keywords)
        except Exception, e:
            logging.exception('Job: %d, game on %s, %s, keywords: %s: stream filter exception' % (job_count, game_datetime, game_name, ','.join(keywords)))
            time.sleep(10)

    # release the auth key so other a job can use
    auth_keys_in_use.pop(str(job_count), None)

    logging.info("Job: %d, game on %s, %s finished. Auth_keys in use: %s" % 
                (job_count, game_datetime, game_name, ' '.join(auth_keys_in_use.values())))


logging.basicConfig(filename='schedule.log', level=logging.INFO,
                    format='%(asctime)s %(message)s')


# Start the scheduler
sched = Scheduler()
sched.start()
print "Scheduler has started",schedule

# auth keys in use
auth_keys_in_use = {}

# scheduled jobs
jobs = {}

# schedule tasks
for count, i in enumerate(schedule):

    game_datetime = parser.parse(
        i['datetime']).replace(tzinfo=None) - timedelta(hours=1)	#??change the time to the server timezone
    
    # skip old game
    if game_datetime < datetime.now():
        continue

    # game name home_away (for database use)
    game_name = "%s_%d%d%d" % ( i['game_name'],game_datetime.year, game_datetime.month, game_datetime.day)

    # logging
    logging.info("Game on %s"%
                (i['datetime']))

    # fetch keywords
    keywords = get_keywords()

    # schedule job
    job = sched.add_date_job(
        connect_streaming_api, game_datetime, args=(keywords, game_datetime, game_name, count))

    jobs[str(count)] = {'datetime':game_datetime, 'job':job}


# keep main thread alive
# unschedule finished job
while True:
    # now = datetime.now()
    # for count, job in jobs.items():
    #     dt = job['datetime']
    #     if (dt + timedelta(minutes=1) <= now):
    #         sched.unschedule_job(job['job'].job)
    #         auth_keys_in_use.pop(key=count, default=None)

    time.sleep(60)

# shutdown scheduler
sched.shutdown()
