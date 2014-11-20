import json
import codecs
import threading
import logging
import time
from dateutil import parser
from dateutil import tz
from datetime import datetime
from datetime import timedelta

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from utility.db import Database
#from worker import Producer


class CustomStreamListener(StreamListener):

    def __init__(self, listener_id, game_datetime, game_name):
        print 'Initialized'
        super(StreamListener, self).__init__()
        self.listener_id = listener_id
        self.db = Database(game_name)
        self.game_name = game_name
        self.count = 0
        self.stop_time = game_datetime + timedelta(hours=5)
        # self.producer = Producer(tube='tweet')

    def on_data(self, tweet):
        tweet = json.loads(tweet)
        logging.info(tweet)
        if 'id_str' in tweet:
            self.count += 1

            # convert date in str to datetime
            create_at_str = tweet['created_at']
            created_at = parser.parse(create_at_str)
            
            # convert UTC to EST
            est = tz.gettz('EST')
            created_at = created_at.astimezone(est)

            # store back to tweet
            tweet['created_at'] = created_at
            #print tweet
            # print tweet
            self.db.insert_tweet(tweet)
        else:
            logging.info("Tweets count: %d" % self.count)
            logging.info("Game %s: Tweet: %s" %
                      (self.game_name, json.dumps(tweet)))
            self.count = 0

        if self.stop_time < datetime.now():
            return False
        
        # self.producer.produce(tweet)
        return True

    def on_error(self, status_code):
        logging.debug("Listener %s: Error %d" %
                      (self.listener_id, status_code))
        return True  # Don't kill the stream

    def on_timeout(self):
        return True  # Don't kill the stream

logging.basicConfig(filename='stream.log', level=logging.INFO,
                         format='%(asctime)s %(message)s')

# class StreamingThread(threading.Thread):

#     def __init__(self, stream, stream_id):
#         threading.Thread.__init__(self)

#         self.stream = stream
#         self.stream_id = stream_id

#     def run(self):
#         try:
#             self.stream.filter(
#                 track=keywords[(self.stream_id * 330):((self.stream_id + 1) * 330)])
#         except Exception, e:
#             logging.exception('stream filter')
#             time.sleep(10)


# if __name__ == '__main__':
#     logging.basicConfig(filename='stream.log', level=logging.DEBUG,
#                         format='%(asctime)s %(message)s')

#     thread_count = 0
#     for key_id, token in keys.items():
#         l = CustomStreamListener(key_id)
#         auth = OAuthHandler(token['consumer_key'], token['consumer_secret'])
#         auth.set_access_token(
#             token['access_token'], token['access_token_secret'])

#         stream = Stream(auth, l)
#         stream_thread = StreamingThread(stream, thread_count)
#         stream_thread.start()

#         thread_count += 1
