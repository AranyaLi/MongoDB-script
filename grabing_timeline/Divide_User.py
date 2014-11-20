# Because there are so many users in the file.
# So, we divide the users into smaller groups.
# And collect one of the list at a time

import json

def get_userIDs(filepath):

	global user_count
	id_list = []
	fin = open(filepath, 'r')
	for line in fin:
		user_count += 1
		id_list.append(line.strip())
	fin.close()
	return id_list



if __name__ == "__main__":

	numOfSets = 20
	user_count = 0
	users = get_userIDs("./src/all_ids")	

	subUsers = {}
	split_pointer_new = 0
	split_range = user_count / numOfSets
	split_pointer_old = split_range

	for i in range(numOfSets):
		subUsers["user_set"+str(i)] = users[split_pointer_new:split_pointer_old]
		split_pointer_new = split_pointer_old
		split_pointer_old = split_pointer_old + split_range

	with open("./src/user_id_sets.py","w") as fout:
		fout.write("user_sets = ")
		json.dump(subUsers, fout, sort_keys = True, indent = 4)
