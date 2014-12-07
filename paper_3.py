import Queue
import CalcFather as calc
from database import Database
from BGLL import BGLL

class TreeNode(object):
	level    = -1
	group_id = -1

	keyword      = ''
	id_set       = ''
	keyword_list = ''

	father_node    = ''
	children_nodes = ''

	def __init__(self):
		self.id_set 		= set()
		self.keyword_list 	= list()
		self.children_nodes = set()


class PaperCluster(object):
	#database config
	db_host = 'localhost'
	db_user = 'root'
	db_pass = 'root'
	db_name = 'papernet'

	#BGLL config
	output_path = './paper/'
	is_weighted	= False

	BGLL 	 = ''
	database = ''

	map_old_to_new = dict()
	map_new_to_old = dict()

	total_level = 0

	trees = set()

	def __init__(self):
		self.BGLL     = BGLL(self.output_path, self.is_weighted)
		self.database = Database(self.db_host, self.db_user, self.db_pass, self.db_name)

		#calc.init_similarity()

	def create_source_file(self):
		f = open('./paper/bgll.txt', 'w+')

		sql = "select pid1, pid2 from paper_paper_relation order by pid1, pid2"
		result = self.database.executeSQL(sql)

		pid_set = set() # the set of all nodes
		for row in result:
			pid_set.add(row[0])
			pid_set.add(row[1])

		pid_list = list(pid_set)
		pid_list.sort()

		num = 0
		for pid in pid_list:
			self.map_old_to_new[pid] = num
			self.map_new_to_old[num] = pid
			num += 1

		for row in result:
			pid1 = self.map_old_to_new[row[0]]
			pid2 = self.map_old_to_new[row[1]]

			f.write('%d %d\n' % (pid1, pid2))

		f.close()

		print 'create_source_file done.'

	def build_trees(self):
		level = self.total_level

		node_dict = dict()
		while level > 0:
			output = self.BGLL.get_level_output(level)

			node_dict[level] = dict()
			for line in output:
				line = line.strip()

				if line == '':
					continue

				(pid, cate) = line.split(' ')
				pid  = int(pid)
				cate = int(cate)

				if cate not in node_dict[level]:
					node_dict[level][cate] = TreeNode()
					node_dict[level][cate].level = level

				
				node_dict[level][cate].id_set.add(self.map_new_to_old[pid])

			level -= 1;

		# tree roots
		for (cate, node) in node_dict[self.total_level].items():
			node.group_id = cate
			self.trees.add(node)

		level = self.total_level
		while level > 1:
			for (cate1, father) in node_dict[level].items():
				for (cate2, child) in node_dict[level - 1].items():
					if father.id_set >= child.id_set:  # Superset
						child.group_id = father.group_id
						child.father_node = father
						father.children_nodes.add(child)

			level -= 1

		print 'build trees done.'

	def find_keywords(self):
		root_set = set()
		for root in self.trees:
			pid_str = ",".join(str(pid) for pid in root.id_set);
			sql = "select content from keyword inner join keyword_paper_relation on keyword.id = kid where pid in (%s)" % pid_str
			result = self.database.executeSQL(sql)
			
			for row in result:
				root.keyword_list.append(row[0])

			root.keyword = calc.CalcFatherv2(root.keyword_list, root_set)
			root_set.add(root.keyword)
			if root.keyword == '':
				print '@@@@', root.keyword_list
			else:
				print root.keyword, root.level, root.group_id

		for root in self.trees:
			is_used_keywords = set() | root_set

			node_queue = Queue.Queue(0)	#0 means no max length queue
			for child in root.children_nodes:
				node_queue.put(child)

			while not node_queue.empty():
				node = node_queue.get()

				pid_str = ",".join(str(pid) for pid in node.id_set);
				sql = "select content from keyword inner join keyword_paper_relation on keyword.id = kid where pid in (%s)" % pid_str
				result = self.database.executeSQL(sql)

				for row in result:
					node.keyword_list.append(row[0])

				node.keyword = calc.CalcFatherv2(node.keyword_list, is_used_keywords)
				is_used_keywords.add(node.keyword)
				if node.keyword == '':
					print '@@@@', node.keyword_list
				else:
					print node.keyword, node.level, node.group_id

				for child in node.children_nodes:
					node_queue.put(child)

		print 'find keywords done.'

	def update_hierarchy_relation(self):
		sql = 'delete from keyword_hierarchy_relation'
		self.database.executeSQL(sql)

		sql = 'update keyword set is_father = 0, father = -1'
		self.database.executeSQL(sql)

		for root in self.trees:
			node_queue = Queue.Queue(0)	#0 means no max length queue
			node_queue.put(root)
			while not node_queue.empty():
				node = node_queue.get()
				for child in node.children_nodes:
					try:
						sql = 'select id from keyword where content = "%s"' % node.keyword
						result = self.database.executeSQL(sql)
						kid1 = result[0][0]

						sql = 'select id from keyword where content = "%s"' % child.keyword
						result = self.database.executeSQL(sql)
						kid2 = result[0][0]

						sql = 'insert into keyword_hierarchy_relation(group_id, father, child) values(%d, %d, %d)' % (node.group_id, kid1, kid2)
						self.database.executeSQL(sql)

						sql = 'update keyword set is_father = 1 where content = "%s"' % node.keyword
						result = self.database.executeSQL(sql)
					except Exception, e:
						# print "###", sql, "###"
						# print node.keyword, "|",child.keyword
						# print e
						pass

					node_queue.put(child)

		print 'update hierarchy relation done.'


	def print_log(self):
		f = open('./paper/analysis.log', 'w+')

		for root in self.trees:
			f.write(root.keyword + "\n")

		f.close


	def start(self):
		self.create_source_file()
		self.BGLL.callBGLL()
		self.total_level = self.BGLL.get_total_level()

		self.build_trees()
		self.find_keywords()
		self.update_hierarchy_relation()
		self.print_log()

pc = PaperCluster()
pc.start()