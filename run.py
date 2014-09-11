import MySQLdb
import os

# database config
db_host = "bit1020.at.bitunion.org"
db_user = "root"
db_pass = "root"
db_name = "papernet"

# read the database keyword_keyword_relation
link1 = list()
link2 = list()
kid_num = dict()
kids_set = set()
try:
	conn = MySQLdb.connect(db_host, db_user, db_pass, db_name)
	cursor = conn.cursor()
	sql = "select kid1, kid2 from keyword_keyword_relation order by kid1, kid2"
	cursor.execute(sql)
	result = cursor.fetchall()
	for row in result:
		link1.append(row[0])
		link2.append(row[1])

		kids_set.add(row[0])
		kids_set.add(row[1])

		kid_num[row[0]] = kid_num.get(row[0], 0) + 1;
		kid_num[row[1]] = kid_num.get(row[1], 0) + 1;
except:
	print "Error happened."
finally:
	if cursor:
		cursor.close()
	if conn:
		conn.close()

# calculate the map between new number and old number
kids_list = list(kids_set)
kids_list.sort();
map1 = dict()
map2 = dict()
num = 0
for kid in kids_list:
	map1[kid] = num
	map2[num] = kid
	num += 1;

try:
	file_path = '/home/cowx/workspace/BGLL/keyword.txt'
	f = open(file_path, 'w+')
	i = 0
	while i < len(link1):
		f.write('%d %d\n' % (map1[link1[i]], map1[link2[i]]))
		i += 1
except:
	print "file", f.name, "failed"
	exit();
finally:
	if f:
		f.close();

os.system("./convert -i keyword.txt -o keyword.bin")
os.system("./community keyword.bin -l -1 > keyword.tree")

output = os.popen("./hierarchy keyword.tree -n")
level = total_level = len(output.readlines()) - 2;

is_used = set()
level_cate = dict()
level_kid = dict()
result =dict()
while level >= 1:
	level_cate[level] = dict()
	level_kid[level] = dict()
	result[level] = dict()

	cate = -1
	kid = -1

	output = os.popen("./hierarchy keyword.tree -l %d" % level)
	for line in output.readlines():
		line = line.strip()

		if not line == '':
			arr = line.split(" ")
			kid  = int(arr[0])
			cate = int(arr[1])

		if not level_cate[level].has_key(cate):
			level_cate[level][cate] = dict()

		# print "level: %d; cate: %d; kid: %d" % (level, cate, kid)
		level_cate[level][cate][kid] = kid_num.get(map2[kid], 0)
		level_kid[level][kid] = cate;

	for cate in level_cate[level]:
		tmp = sorted(level_cate[level][cate].iteritems(), key = lambda d:d[1], reverse = True)
		for kid in tmp:
			if kid[0] not in is_used:
				is_used.add(kid[0])
				result[level][cate] = kid[0]
				break

	# print "level", level, "done."
	level = level - 1




try:
	conn = MySQLdb.connect(db_host, db_user, db_pass, db_name)
	cursor = conn.cursor()
	sql = "update keyword set father = -1"
	cursor.execute(sql)

	file_path = '/home/cowx/workspace/BGLL/keyword.tree'
	f = open(file_path, 'r')

	level = 0;
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

		# print level, kid, cate
except:
	print "Error happened.", level, kid, cate, sql
finally:
	if cursor:
		cursor.close()
	if conn:
		conn.close()

print "BGLL DONE."