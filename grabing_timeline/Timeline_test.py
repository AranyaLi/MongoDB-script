import tweepy
import signal
import logging
import time
import httplib
import json
from threading import Thread 
import thread
from tweepy import Cursor

from src.auth_keys import keys
from src.user_id_sets import user_sets
from db import Database

# Tweepy is a great easy to use twitter library for python. 
# MongoDB is a great way to save your data. 
# However tweepy does not give the raw data from twitter
# so you need to add a monkeypatch (love that term) at the top of your project file:
# ============================================================================
@classmethod
def parse(cls, api, raw):
	status = cls.first_parse(api, raw)
	setattr(status, 'json', json.dumps(raw))
	return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse
# ============================================================================
def get_api(a):

	auth = tweepy.OAuthHandler(a['consumer_key'], a['consumer_secret'])
	auth.set_access_token(a['access_token'], a['access_token_secret'])
	api = tweepy.API(auth)
	return api

def get_auth_account():
	# get an auth key that is not in use
    for key_id, token in keys.items():			
        if key_id not in auth_keys_in_use.keys():
            return key_id
        elif (time.time() - auth_keys_in_use[key_id]) > (15*60):
			return key_id
    return None

def get_userIDs(user_id_sets_id):

	global user_count
# 	id_list = []
# 	fin = open(filepath, 'r')
# 	for line in fin:
# 		user_count += 1
# 		id_list.append(line)
# #		print line
# 	fin.close()
	sets = "user_set"+str(user_id_sets_id)
	id_list = user_sets[sets]
	user_count = len(id_list)
	return id_list

# ============================================================================

# This API can get 200 tweets each request, 300 requests per 15 minutes
# From each user we can get up to 3200 tweets which require 16 requests
# Therefore, for each user, it requires at most 16 requests
def get_user_timeline(users, threadNum):

	global auth_keys_in_use	
	global total_request
	global exception_count
	global collectedFile
	global user_id_sets_id
	global current_numOf_user

	logging.info("Thread %d start collecting timeline of users.\n" % threadNum)
	request_count = 0

# -----------------------------------	
	lock.acquire()
	auth_id = get_auth_account()
	lock.release()
# -----------------------------------	
	start_time = time.time()
# -----------------------------------	
	lock.acquire()
	auth_keys_in_use[auth_id] = start_time
	print auth_keys_in_use[auth_id] 
	lock.release()
# -----------------------------------	
	
	api = get_api(keys[auth_id])
	logging.info("Switch Auth key to %s\n" % auth_id)
	# print "Thread %d Switch Auth key to %s\n" % (threadNum, auth_id)	

	for user in users:
		current_numOf_user += 1
		# -----------------------------------	
		lock.acquire()
		logging.info("Thread %d Collecting the timeline of user %s... \n" % ( threadNum, user))
		lock.release()
		# -----------------------------------	
		# print "Thread %d Collecting the timeline of user %s... \n" % ( threadNum, user)
		db = Database("user_" + user.strip(), user_id_sets_id)
		count = 0	#the number of tweets of an user


		#check if the api has reach its rate limit, if true replace api
		#160-16 --> if the api continue to be used, we may not have enough requests to get all the timeline of the next user
		#---------------------------------------------------------------------
		if request_count >= (160-16):
			while (True):
				# -----------------------------------	
				lock.acquire()
				auth_id = get_auth_account()
				lock.release()
				# -----------------------------------	
				if auth_id != None:
					break
				# -----------------------------------	
				lock.acquire()
				info.logging("Put Thread%d to sleep for 20 seconds.\n" % threadNum)
				lock.release()
				# -----------------------------------
				time.sleep(20)
				# -----------------------------------	
				lock.acquire()
				info.logging("Wake up Thread%d.\n" % threadNum)
				lock.release()
				# -----------------------------------

			start_time = time.time()
			# -----------------------------------	
			lock.acquire()
			auth_keys_in_use[auth_id] = start_time
			# print auth_keys_in_use[auth_id]
			lock.release()
			# -----------------------------------	
			api = get_api(keys[auth_id])
			request_count = 0
			# -----------------------------------	
			lock.acquire()
			logging.info("Thread%d Switch Auth key to %s\n" % (threadNum, auth_id))
			lock.release()
			# -----------------------------------
			# print "Thread%d Switch Auth key to %s\n" % (threadNum, auth_id)
		#---------------------------------------------------------------------

		try:
			#this will generate requests automatically to fetch up to 3200 tweets of a user.
			for t in Cursor(api.user_timeline, id=user,count=200).items():
				tweet = json.loads(t.json)
				db.insert_tweet(tweet)
				count = count + 1
				if (count % 200 == 0):
					request_count += 1
					print "Thread%d, %d" % (threadNum, request_count)
				# -----------------------------------	
					lock.acquire()
					total_request += 1
					lock.release()
				# -----------------------------------	

			if(count!=3200):
				request_count += 1
				print "Thread%d, %d" % (threadNum, request_count)
			# -----------------------------------
			lock.acquire()
			collectedFile.write(user.strip()+'\n')
			lock.release()
			# -----------------------------------


		except httplib.IncompleteRead as e:
			# -----------------------------------
			lock.acquire()
			collectedFile.write(user.strip()+'\n')			# Incomplete read user also in collected_userID
			logging.exception("Thread%d IncompleteRead ERROR! USERID=%s.  %s.\n" % (threadNum, user, e))
			exception_count += 1
			lock.release()
			# -----------------------------------
			# request_count = 160 	#force the program to change for another auth

		except tweepy.TweepError as e:
			# -----------------------------------
			lock.acquire()
			logging.exception("Thread%d Tweepy ERROR! USERID=%s.  %s.\n" % (threadNum, user, e))
			exception_count += 1
			lock.release()
			# -----------------------------------

#===========================================================================

# def signal_handler(signal, frame):	#does not work
# 	global begin_time
# 	global end_time
# 	end_time = time.time()
# 	print "Signal Test+++++++++++++++++++++++\n"
# 	logging.info("End of task.\n")
# 	logging.info("Threads used: %d.\n" % numOfThreads)
# 	logging.info("Total number of users : %d.\n" % user_count)
# 	logging.info("Total number of requests : %d.\n" % total_request)
# 	logging.info("Total number of exceptions : %d.\n" % exception_count)
# 	logging.info("Time used : %s seconds.\n" % str(end_time-begin_time))
# 	logging.info("Time used per user : %s.\n" % str((int)(end_time-begin_time)/user_count))
# 	# sys.exit(0)


def release_api():
	# global user_count
	# global current_numOf_user
	# global auth_keys_in_use

	while flag == 0: 	# There is no need for mutual exclusion here, because those can be released are already not used. 
		for key_id in auth_keys_in_use.keys():
			if (time.time() - auth_keys_in_use[key_id]) > (15*60):
				print "Release API %s" % key_id
				auth_keys_in_use.pop(key_id)
		print "Release API thread sleep for 10 seconds.\n"
		time.sleep(60)


#===========================================================================
# Main function
if __name__ == "__main__":
	collectedFile = open("./collected_userID",'w')		# Record the user's that has been successfully collected
	begin_time = time.time()
	logging.basicConfig(filename='stream.log', level=logging.INFO,
                         format='%(asctime)s %(message)s')

	# signal.signal(signal.SIGINT, signal_handler)
# -----------------------------------	
	lock=thread.allocate_lock()
# -----------------------------------	

	user_count = 0
	total_request = 0
	auth_keys_in_use = {}
	exception_count = 0    					# corresponds to how many users' timeline are incomplete
	user_id_sets_id = 3						# indicates which users subset to use.
#	num_of_sets = 20  						# number of user sets in ./src/user_id_sets.py. Corresponds to numOfSets in Divide_User.py
	users = get_userIDs(user_id_sets_id)
	current_numOf_user = 0	
	flag = 0 								# flag is used to kill the release API thread

	print "user_count = %d" % user_count

	# --------------------------------------------------------------
	# Multithreading
	# Split users into 10 threads
	threadList = []
	numOfThreads = 10

	subUsers = []
	split_pointer_new = 0
	split_range = user_count / numOfThreads
	split_pointer_old = split_range

	for i in range(numOfThreads):
		subUsers.append(users[split_pointer_new:split_pointer_old])
		split_pointer_new = split_pointer_old
		split_pointer_old = split_pointer_old + split_range

		
	# subUsers.append(users[:split_pointer])
	# subUsers.append(users[split_pointer:])

	for i in range(numOfThreads):
		t = Thread(target = get_user_timeline, args = (subUsers[i],i))
		threadList.append(t)
		t.start()
	releaseAPIThread = Thread(target = release_api)
	releaseAPIThread.start()

	# t1 = Thread(target = get_user_timeline, args = (subUsers[0],0))
	# t2 = Thread(target = get_user_timeline, args = (subUsers[1],1))
	# t1.start()
	# t2.start()
	# threadList.append(t1)
	# threadList.append(t2)

	for th in threadList:
		th.join()
	flag = 1
	releaseAPIThread.join()

	# get_user_timeline(users,1)

	collectedFile.close()
	# --------------------------------------------------------------
	# Statistics
	end_time = time.time()
	logging.info("End of task.\n")
	logging.info("Threads used: %d.\n" % numOfThreads)
	logging.info("Total number of users : %d.\n" % user_count)
	logging.info("Total number of requests : %d.\n" % total_request)
	logging.info("Total number of exceptions : %d.\n" % exception_count)
	logging.info("Time used : %s seconds.\n" % str(end_time-begin_time))
	logging.info("Time used per user : %s.\n" % str((int)(end_time-begin_time)/user_count))