from pymongo import MongoClient


class Database(object):

    def __init__(self, game_name):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.IndyCar	#Database Name, Change it.
        self.tweet = self.db[game_name]
        self.profile = self.db.profile

    def insert_tweet(self, tweet):
        if not self.is_tweet_exists(tweet['id_str']):
            self.tweet.insert(tweet)

    def insert_profile(self, profile):
        self.profile.update({"id_str": profile['id_str']},
                            {"$set": {"profile": profile}},
                            upsert=True)

    def insert_friends(self, uid, friends):
        self.profile.update({"id_str": uid},
                            {"$set": {"friends": friends}},
                            upsert=True)

    def insert_followers(self, uid, followers):
        self.profile.update({"id_str": uid},
                            {"$set": {"followers": followers}},
                            upsert=True)

    def is_tweet_exists(self, tid):
        result = self.tweet.find({"id_str": tid}).limit(1)
        if result.count() == 0:
            return False
        return True

    def is_key_exists_in_profile(self, uid, key):
        result = self.profile.find({"$and": [{"id_str": uid},
                                             {key: {"$exists": True}}]}).limit(1)
        if result.count() == 0:
            return False
        return True
