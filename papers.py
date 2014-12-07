import MySQLdb
import os
import CalcFather as calc

# database config
db_host = "localhost"
db_user = "root"
db_pass = "root"
db_name = "papernet"

# global database connection and cursor
conn = MySQLdb.connect(db_host, db_user, db_pass, db_name)
cursor = conn.cursor()

calc.init_similarity()

# read the database paper_paper_relation and write into paper.txt
map1 = dict()	# from old to new
map2 = dict()	# from new to old

file_path = '/home/cowx/workspace/BGLL/paper/paper.txt'
f = open(file_path, 'w+')

sql = "select pid1, pid2 from paper_paper_relation order by pid1, pid2"
cursor.execute(sql)
result = cursor.fetchall()

pid_set = set() # the set of all nodes
for row in result:
	pid_set.add(row[0])
	pid_set.add(row[1])

pid_list = list(pid_set)
pid_list.sort()

num = 0
for pid in pid_list:
	map1[pid] = num
	map2[num] = pid
	num += 1

for row in result:
	pid1 = map1[row[0]]
	pid2 = map1[row[1]]

	f.write('%d %d\n' % (pid1, pid2))

f.close()

# call the BGLL method
os.system("BGLL/convert -i paper/paper.txt -o paper/paper.bin")
os.system("BGLL/community paper/paper.bin -l -1 > paper/paper.tree")

# read the result of BGLL
output = os.popen("BGLL/hierarchy paper/paper.tree -n")
level = total_level = len(output.readlines()) - 2

# find the father of each node
is_used = set()				# whether used the node as a category
paper_children   = dict()	# record category in every level contains which papers 
keyword_father   = dict()	# record which keyword presents the catogory in every level
keyword_children = dict()	# record category in every level contains which keywords 
while level >= 1:
	paper_children[level]   = dict()
	keyword_father[level]   = dict()
	keyword_children[level] = dict()

	# record category in every level contains which papers  
	output = os.popen("BGLL/hierarchy paper/paper.tree -l %d" % level)
	for line in output.readlines():
		line = line.strip()

		if line == '':
			continue

		arr = line.split(" ")
		pid  = int(arr[0])
		cate = int(arr[1])

		if not paper_children[level].has_key(cate):
			paper_children[level][cate] = set()

		paper_children[level][cate].add(str(map2[pid]))

	for cate in paper_children[level]:
		pid_set = paper_children[level][cate]
		pid_str = ','.join(pid_set)

		sql = 'select kid, content from keyword_paper_relation inner join keyword on kid = keyword.id where pid in (' + pid_str + ')'
		cursor.execute(sql)
		sql_result = cursor.fetchall()
		
		if len(sql_result) != 0:
			kid_set = set()
			content_list = list()

			for row in sql_result:
				kid_set.add(str(row[0]))
				content_list.append(row[1])
		
			content = calc.CalcFatherv2(content_list, is_used)
			is_used.add(content)

			# find the keyword id of the content
			sql = "select id from keyword where content = '" + content + "'"
			cursor.execute(sql)
			row = cursor.fetchone()

			if row is not None:
				is_used.add(str(row[0]))

				# remvoe the used keyword
				for kid in is_used:
					if kid in kid_set:
						kid_set.remove(kid)

				keyword_father[level][cate] = row[0]
				keyword_children[level][cate] = kid_set

			print content

	level = level - 1

# update the database according to the result
sql = "update keyword set father = -1, is_father = 0"
cursor.execute(sql)

level = total_level
is_updated = set()


print is_updated

while level >= 1:
	for (cate, father) in keyword_father[level].items():
		is_updated.add(str(father))

		sql = "update keyword set is_father = 1 where id = %s" % father
		cursor.execute(sql)

	for (cate, father) in keyword_father[level].items():
		if len(keyword_children[level][cate]) == 0:
			continue
		
		for child in keyword_children[level][cate]:
			if child not in is_updated:
				sql = "update keyword set father = %s where id = %s" % (father, child)
				cursor.execute(sql)
				# print sql
			else:
				print child, father
				pass

		sql = "update keyword set is_father = 1 where id = %s" % father
		cursor.execute(sql)		

	level = level - 1

conn.commit()

if cursor:
	cursor.close()
if conn:
	conn.close()

print "BGLL DONE."
