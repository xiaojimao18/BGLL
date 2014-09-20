import MySQLdb
import os


# database config
db_host = "bit1020.at.bitunion.org"
db_user = "root"
db_pass = "root"
db_name = "papernet"

conn = ''		# global database connection
cursor = ''		# global database cursor



def CalcFather(wordlist, usedset):
        """
        Given wordlist and usedset, output the father word
        """
	score = {}
	for word in wordlist:
		p = 0
		for other in wordlist:
			p += WordSimilarty(word, other)
		if score.has_key(word):
			score[word] += p
		else:
			score[word] = p
	bestword = ""
	bestscore = 0
	for word in wordlist:
		if word in usedset:
			continue
		if score[word] > bestscore or (score[word] == bestscore and len(word) < len(bestword)):
			bestword = word
			bestscore = score[word]
	return bestword

def WordSimilarty(wa, wb):
	lena = len(wa)
	lenb = len(wb)
	f = list()
	for i in range(lena + 1):
		j = list([0 for k in range(lenb + 1)])
		f.append(j)

	for i in range(1, lena + 1):
		for j in range(1, lenb + 1):
			if wa[i - 1] == wb[j - 1]:
				f[i][j] = f[i - 1][j - 1] + 1
			else:
				f[i][j] = max(f[i - 1][j], f[i][j - 1])
	return 1.0 * f[lena][lenb] / max(lena, lenb);

def IsSameWords(wa, wb):
        sim = (1 - WordSimilarty(wa, wb)) * max(len(wa), len(wb))
        if sim < 4.0:
                return True
        else:
                return False

def DuplicatedWords(wordlist, srcWord):
        """
        find similar words with srcWord in the wordlist
        output list
        """
        dup = list()
        for word in wordlist:
                if IsSameWords(word, srcWord):
                        dup.append(word)
        return dup

def DuplicatedWords4ALL(wordlist):
        """
        find all duplicated words in the wordlist
        output set
        """
        wordSim = {}
        for word in wordlist:
                simList = list()
                for other in wordlist:
                        if IsSameWords(word, other) and word != other:
                                simList.append(other)
                wordSim[word] = simList
        dupSet = set()
        for word in wordlist:
                if word in dupSet:
                        continue
                simSet = set()
                GetSimSet(word, wordSim, simSet)
                father = CalcFather(list(simSet), set())
                for other in simSet:
                        if other != father:
                                dupSet.add(other)
        return dupSet

def GetSimSet(word, wordSim, simSet):
        if word in simSet:
                return
        simSet.add(word)
        for other in wordSim[word]:
                GetSimSet(other, wordSim, simSet)

#wordlist = ['cluster', 'clusters', 'clustering', 'community' ,'communities', 'recommender', 'recommendation', 'recommend']
#print DuplicatedWords4ALL(wordlist)        


# read the database author_author_relation
aid_set = set()  # the set of all nodes
try:
	file_path = '/home/cowx/workspace/BGLL/author/author.txt'
	f = open(file_path, 'w+')

	conn = MySQLdb.connect(db_host, db_user, db_pass, db_name)
	cursor = conn.cursor()
	sql = "select aid1, aid2, num from author_author_relation order by aid1, aid2"
	cursor.execute(sql)
	result = cursor.fetchall()

	for row in result:
		aid_set.add(row[0])
		aid_set.add(row[1])

		f.write('%d %d %d\n' % (row[0], row[1], row[2]))
except:
	print "***********************************************"
	print "Error happened while creating the file author.txt"
	print "***********************************************"
finally:
	if f:
		f.close()

# calculate the map between new number and old number
aid_list = list(aid_set)
aid_list.sort()

map1 = dict()
map2 = dict()
num = 0
for aid in aid_list:
	map1[aid] = num
	map2[num] = aid
	num += 1


os.system("./convert -i author/author.txt -o author/author.bin -w")
os.system("./community author/author.bin -w -l -1 > author/author.tree")


output = os.popen("./hierarchy author/author.tree -n")
level = total_level = len(output.readlines()) - 2


level_cate = dict()		# record the category contains which nodes in every level
is_used = set()			# whether used the node as a category
result =dict()			# recode which node presents the catogory in every level
while level >= 1:
	level_cate[level] = dict()
	result[level] = dict()

	cate = -1
	aid = -1

	output = os.popen("./hierarchy author/author.tree -l %d" % level)
	for line in output.readlines():
		line = line.strip()

		if line == '':
			continue

		arr = line.split(" ")
		aid  = int(arr[0])
		cate = int(arr[1])

		if not level_cate[level].has_key(cate):
			level_cate[level][cate] = set()

		level_cate[level][cate].add(str(aid))

	for cate in level_cate[level]:
		a_set = level_cate[level][cate]
		set_str = ','.join(a_set)

		try:
			sql = 'select content from author_keyword_relation inner join keyword on kid = keyword.id where aid in (' + set_str + ')'
			cursor.execute(sql)
			sql_result = cursor.fetchall()
			
			if len(sql_result) != 0:
				content_list = list()

				for row in sql_result:
					content_list.append(row[0])
			
				content = CalcFather(content_list, is_used)
				print content
		except:
			print "***********************************************"
			print "select keyword content error"
			print "***********************************************"


	level = level - 1




# try:
# 	sql = "update author set father = -1"
# 	cursor.execute(sql)

# 	file_path = '/home/cowx/workspace/BGLL/author/author.tree'
# 	f = open(file_path, 'r')

# 	level = 0
# 	for line in f.readlines():
# 		line = line.strip()

# 		if line == '':
# 			continue

# 		arr = line.split(' ')
# 		kid = int(arr[0])
# 		cate = int(arr[1])

# 		if kid == 0:
# 			level = level + 1

# 		if level >= total_level:
# 			break

# 		if level == 1 and result[level].has_key(cate):
# 			sql = "update author set father = %d where id = %d" % (map2[result[level][cate]], map2[kid])
# 		elif level != 1 and result[level].has_key(cate) and result[level - 1].has_key(kid):
# 			sql = "update author set father = %d where id = %d" % (map2[result[level][cate]], map2[result[level - 1][kid]])

# 		cursor.execute(sql)

# except:
# 	print "***********************************************"
# 	print "Error happened while updating the database"
# 	print "***********************************************"
# finally:
# 	if cursor:
# 		cursor.close()
# 	if conn:
# 		conn.close()

print "BGLL DONE."
