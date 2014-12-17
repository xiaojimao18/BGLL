import MySQLdb
import os
import CalcFather as calc

# database config
db_host = "bit1020.at.bitunion.org"
db_user = "root"
db_pass = "root"
db_name = "papernet"

# global database connection and cursor
conn = MySQLdb.connect(db_host, db_user, db_pass, db_name)
cursor = conn.cursor()

# init the similarity module
calc.init_similarity()

# read the database author_author_relation and write into author.txt
map1 = dict()	# from old to new
map2 = dict()	# from new to old

file_path = '/home/cowx/workspace/BGLL/author/author.txt'
f = open(file_path, 'w+')

sql = "select aid1, aid2, num from author_author_relation order by aid1, aid2"
cursor.execute(sql)
result = cursor.fetchall()

aid_set = set() # the set of all nodes
for row in result:
	aid_set.add(row[0])
	aid_set.add(row[1])

aid_list = list(aid_set)
aid_list.sort()

num = 0
for aid in aid_list:
	map1[aid] = num
	map2[num] = aid
	num += 1

for row in result:
	aid1 = map1[row[0]]
	aid2 = map1[row[1]]

	f.write('%d %d %d\n' % (aid1, aid2, row[2]))

f.close()

# call the BGLL method
os.system("BGLL/convert -i author/author.txt -o author/author.bin -w")
os.system("BGLL/community author/author.bin -w -l -1 > author/author.tree")

# read the result of BGLL
output = os.popen("BGLL/hierarchy author/author.tree -n")
level = total_level = len(output.readlines()) - 2

# find the father of each node
is_used = set()				# whether used the node as a category
author_children  = dict()	# record category in every level contains which authors 
keyword_father   = dict()	# record which keyword presents the catogory in every level
keyword_children = dict()	# record category in every level contains which keywords 
while level >= 1:
	author_children[level]  = dict()
	keyword_father[level]   = dict()
	keyword_children[level] = dict()

	# record category in every level contains which authors  
	output = os.popen("BGLL/hierarchy author/author.tree -l %d" % level)
	for line in output.readlines():
		line = line.strip()

		if line == '':
			continue

		arr = line.split(" ")
		aid  = int(arr[0])
		cate = int(arr[1])

		if not author_children[level].has_key(cate):
			author_children[level][cate] = set()

		author_children[level][cate].add(str(map2[aid]))

	for cate in author_children[level]:
		a_set = author_children[level][cate]
		aid_str = ','.join(a_set)

		sql = 'select kid, content from author_keyword_relation inner join keyword on kid = keyword.id where aid in (' + aid_str + ')'
		cursor.execute(sql)
		sql_result = cursor.fetchall()
		
		if len(sql_result) != 0:
			kid_set = set()
			content_list = list()

			for row in sql_result:
				kid_set.add(str(row[0]))
				content_list.append(row[1])
		
			content = calc.CalcFather(content_list, is_used)
			is_used.add(content)

			# find the keyword id of the content
			sql = "select id from keyword where content = '"+ content +"'"
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

			print content, aid_str

	level = level - 1

# update the database according to the result
sql = "update keyword set father = -1, is_father = 0"
cursor.execute(sql)

level = total_level
while level >= 1:
	for cate in keyword_father[level]:
		if len(keyword_children[level][cate]) == 0:
			continue

		kid_str = ",".join(keyword_children[level][cate])
		sql = "update keyword set father = %d where id in (%s)" % (keyword_father[level][cate], kid_str)
		cursor.execute(sql)

		sql = "update keyword set is_father = 1 where id = %s" % keyword_father[level][cate]
		cursor.execute(sql)

	level = level - 1

conn.commit()

if cursor:
	cursor.close()
if conn:
	conn.close()

print "BGLL DONE."
