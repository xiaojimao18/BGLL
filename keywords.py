import MySQLdb
import os

# database config
db_host = "bit1020.at.bitunion.org"
db_user = "root"
db_pass = "root"
db_name = "papernet"

conn = ''		# global database connection
cursor = ''		# global database cursor

# read the database keyword_keyword_relation and write into keyword.txt
kid_num = dict()	# the degree of each node
kid_set = set()		# the set of all nodes
map1 = dict()		# from old to new
map2 = dict()		# from new to old
try:
	file_path = '/home/cowx/workspace/BGLL/keyword/keyword.txt'
	f = open(file_path, 'w+')

	conn = MySQLdb.connect(db_host, db_user, db_pass, db_name)
	cursor = conn.cursor()
	sql = "select kid1, kid2, num from keyword_keyword_relation order by kid1, kid2"
	cursor.execute(sql)
	result = cursor.fetchall()

	for row in result:
		kid_set.add(row[0])
		kid_set.add(row[1])

	kid_list = list(kid_set)
	kid_list.sort()

	num = 0
	for kid in kid_list:
		map1[kid] = num
		map2[num] = kid
		num += 1

	for row in result:
		kid1 = map1[row[0]]
		kid2 = map1[row[1]]

		kid_num[kid1] = kid_num.get(kid1, 0) + row[2]
		kid_num[kid2] = kid_num.get(kid2, 0) + row[2]

		f.write('%d %d %d\n' % (kid1, kid2, row[2]))
except:
	print "***********************************************"
	print "Error happened while creating the file keyword.txt"
	print "***********************************************"
finally:
	if f:
		f.close()

# call the BGLL method
os.system("./convert -i keyword/keyword.txt -o keyword/keyword.bin -w")
os.system("./community keyword/keyword.bin -w -l -1 > keyword/keyword.tree")

# read the result of BGLL
output = os.popen("./hierarchy keyword/keyword.tree -n")
level = total_level = len(output.readlines()) - 2


level_cate = dict()		# record the category contains which nodes in every level
is_used = set()			# whether used the node as a category
result =dict()			# recode which node presents the catogory in every level
while level >= 1:
	level_cate[level] = dict()
	result[level] = dict()

	cate = -1
	kid = -1

	output = os.popen("./hierarchy keyword/keyword.tree -l %d" % level)
	for line in output.readlines():
		line = line.strip()

		if not line == '':
			arr = line.split(" ")
			kid  = int(arr[0])
			cate = int(arr[1])

		if not level_cate[level].has_key(cate):
			level_cate[level][cate] = dict()

		level_cate[level][cate][kid] = kid_num.get(map2[kid], 0)

	for cate in level_cate[level]:
		tmp = sorted(level_cate[level][cate].iteritems(), key = lambda d:d[1], reverse = True)
		for kid in tmp:
			if kid[0] not in is_used:
				is_used.add(kid[0])
				result[level][cate] = kid[0]
				break

	level = level - 1


try:
	sql = "update keyword set father = -1"
	cursor.execute(sql)

	file_path = '/home/cowx/workspace/BGLL/keyword/keyword.tree'
	f = open(file_path, 'r')

	level = 0
	for line in f.readlines():
		line = line.strip()

		if line == '':
			continue

		arr = line.split(' ')
		kid = int(arr[0])
		cate = int(arr[1])

		if kid == 0:
			level = level + 1

		if level >= total_level:
			break

		if level == 1 and result[level].has_key(cate):
			sql = "update keyword set father = %d where id = %d" % (map2[result[level][cate]], map2[kid])
		elif level != 1 and result[level].has_key(cate) and result[level - 1].has_key(kid):
			sql = "update keyword set father = %d where id = %d" % (map2[result[level][cate]], map2[result[level - 1][kid]])

		cursor.execute(sql)
except:
	print "***********************************************"
	print "Error happened while updating the database"
	print "***********************************************"
finally:
	if cursor:
		cursor.close()
	if conn:
		conn.commit()
		conn.close()

print "BGLL DONE."