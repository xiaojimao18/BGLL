import MySQLdb
import os
import CalcFather as calc


##################config#################
# database config
db_host = "localhost"
db_user = "root"
db_pass = "root"
db_name = "papernet"

# file paths
sourceFile = './paper/bgll.txt'
binFile    = './paper/bgll.bin'
treeFile   = './paper/bgll.tree'
##################config#################


##################global variables#################
# database connection and cursor
conn   = None		# init in connectDB function
cursor = None

# map between old ids(in database) and new ids(in BGLL)
map_old_to_new = dict()	# from old to new
map_new_to_old = dict()	# from new to old

# total level of hierarchy cluster
total_level = 0

# store the result
result_paper_pid  = dict()
result_paper_cate = dict()
##################global variables#################

def connectDB():
	global conn
	global cursor

	conn = MySQLdb.connect(db_host, db_user, db_pass, db_name)
	cursor = conn.cursor()

def closeDB():
	if cursor:
		cursor.close

	if conn:
		conn.commit()
		conn.close()

def executeSQL(sql):
	if cursor is None:
		connectDB()

	cursor.execute(sql)
	return cursor.fetchall()


# read the database paper_paper_relation and write into paper.txt
def createSourceFile():
	global map_old_to_new
	global map_new_to_old

	f = open(sourceFile, 'w+')

	sql = "select pid1, pid2 from paper_paper_relation order by pid1, pid2"
	result = executeSQL(sql)

	pid_set = set() # the set of all nodes
	for row in result:
		pid_set.add(row[0])
		pid_set.add(row[1])

	pid_list = list(pid_set)
	pid_list.sort()

	num = 0
	for pid in pid_list:
		map_old_to_new[pid] = num
		map_new_to_old[num] = pid
		num += 1

	for row in result:
		pid1 = map_old_to_new[row[0]]
		pid2 = map_old_to_new[row[1]]

		f.write('%d %d\n' % (pid1, pid2))

	f.close()

	print 'createSourceFile done.'

# call the BGLL method
def callBGLL():
	global total_level

	os.system("BGLL/convert -i %s -o %s" % (sourceFile, binFile))
	os.system("BGLL/community %s -l -1 > %s" % (binFile, treeFile))

	output = os.popen("BGLL/hierarchy %s -n" % treeFile)
	total_level = len(output.readlines()) - 2

	print 'callBGLL done.'

# get the result in all level
def getAllResult():
	global result_paper_cate
	global result_paper_pid

	for i in range(0, total_level + 1):
		result_paper_cate[i] = dict()
		result_paper_pid[i]  = dict()

		# output format: pid cate
		output = os.popen("BGLL/hierarchy %s -l %d" % (treeFile, i))
		for line in output:
			line = line.strip()

			if line == '':
				continue

			(pid, cate) = line.split(' ')
			pid = map_new_to_old[int(pid)]		# change the new pid to old pid

			if cate not in result_paper_cate[i]:
				result_paper_cate[i][cate] = set()

			result_paper_cate[i][cate].add(pid)
			result_paper_pid[i][pid] = cate


def clearHierarchyRelation():
	sql = "delete from keyword_hierarchy_relation"
	executeSQL(sql)

def updateHierarchyRelation(group, pid_set, level, father, is_selected):
	if len(pid_set) == 0:
		return

	pid_str = ",".join(str(pid) for pid in pid_set)
	sql = 'select keyword.id, content from keyword inner join keyword_paper_relation on kid = keyword.id where pid in (%s)' % pid_str
	try:
		result = executeSQL(sql)
	except Exception, e:
		print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
		print sql
		print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
	
	id_set = set()
	content_list = list()
	for row in result:
		if row[1] not in is_selected:
			id_set.add(row[0])
			content_list.append(row[1])

	if father != -1:
		for kid in id_set:
			try:
				sql = 'insert into keyword_hierarchy_relation(group_id, father, child) values(%d, %d, %d)' % (int(group), father, kid)
				executeSQL(sql)
			except Exception, e:
				if str(e).find('1062') != -1:  #duplicate key
					sql = 'update keyword_hierarchy_relation set father = %d where group_id = %d and child = %d' % (father, int(group), kid)
					executeSQL(sql)
				else:
					print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
					print sql
					print e
					print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'


	content = calc.CalcFatherv2(content_list, is_selected)

	sql = 'select id from keyword where content = "%s"' % content
	try:
		result = executeSQL(sql)
		is_selected.add(content)
	except Exception, e:
		print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
		print sql
		print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'

	print "%s %d %d" %(content, father, level)
	# if content == "":
		# print "[pids:%s content:%s]" % (pid_set, content_list)

	if level >= 1 and content != "":
		cate_set = set()

		for pid in pid_set:
			cate_set.add(result_paper_pid[level - 1][pid])

		for cate in cate_set:
			updateHierarchyRelation(group, result_paper_cate[level - 1][cate], level - 1, result[0][0], is_selected)
	

# Program starts here
def start():
	connectDB()

	calc.init_similarity()

	createSourceFile()
	callBGLL()
	getAllResult()
	
	clearHierarchyRelation()

	# group: the id of each tree
	for (group, pid_set) in result_paper_cate[total_level].items():
		print "======================level:%s====================" % group
		is_selected = set()
		updateHierarchyRelation(group, pid_set, total_level, -1, is_selected)

	closeDB()


start()
print 'DONE.'
