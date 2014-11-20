from pymongo import MongoClient


class Database(object):

	def __init__(self, user_name, user_subset_count):
		self.client = MongoClient('localhost', 27017)
		db_name = "User_Timeline_Data_subUser" + str(user_subset_count)
		self.db = self.client[db_name]				#Database Name, Change it.
		self.user_timeline = self.db[user_name]

	def insert_tweet(self, tweet):
        # if not self.is_tweet_exists(tweet['id_str']):
		self.user_timeline.insert(tweet)

    # def is_tweet_exists(self, tid):
    #     result = self.tweet.find({"id_str": tid}).limit(1)
    #     if result.count() == 0:
    #         return False
    #     return True
	def insert_collected_ID(self, user):
		self.user_timeline.insert(user)
