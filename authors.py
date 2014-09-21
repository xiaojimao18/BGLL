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


# read the database author_author_relation and write into author.txt
aid_set = set() # the set of all nodes
map1 = dict()	# from old to new
map2 = dict()	# from new to old
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
except:
	print "***********************************************"
	print "Error happened while creating the file author.txt"
	print "***********************************************"
finally:
	if f:
		f.close()

# call the BGLL method
os.system("./convert -i author/author.txt -o author/author.bin -w")
os.system("./community author/author.bin -w -l -1 > author/author.tree")

# read the result of BGLL
output = os.popen("./hierarchy author/author.tree -n")
level = total_level = len(output.readlines()) - 2


level_cate = dict()		# record the category contains which nodes in every level
is_used = set()			# whether used the node as a category
result =dict()			# recode which node presents the catogory in every level
while level >= 1:
	level_cate[level] = dict()
	result[level] = dict()

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

		level_cate[level][cate].add(str(map2[aid]))

	for cate in level_cate[level]:
		a_set = level_cate[level][cate]
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
		
			content = CalcFather(content_list, is_used)
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

				result[level][cate] = row[0]
				result[level][str(cate) + 'kids'] = kid_set

			print content

	level = level - 1

# update the database according to the result

sql = "update keyword set father = -1"
cursor.execute(sql)

level = total_level
while level >= 1:
	for cate in result[level]:
		# judge if cate is a number
		if not isinstance(cate, (int, long)):
			continue

		if len(result[level][str(cate) + 'kids']) == 0:
			continue

		kid_str = ",".join(result[level][str(cate) + 'kids'])
		sql = "update keyword set father = %d where id in (%s)" % (result[level][cate], kid_str)
		cursor.execute(sql)

	level = level - 1

conn.commit()

if cursor:
	cursor.close()
if conn:
	conn.close()

print "BGLL DONE."
