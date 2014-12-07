import math
import re

#SimiarityMethod = "EditDistance"
SimiarityMethod = "WordVector"

word2vec = {}
len_vec = 0

def init_similarity():
        """
        read word2vector data
        """
        global word2vec
        global len_vec
        filename = "abstract_vector.dat"
        lines = [line for line in file(filename)]
        (num_words, len_vec) = map(int, lines[0].strip().split(' '))
        for line in lines[1:]:
                items = line.strip().split(' ')
                vector = map(float, items[1:])
                word2vec[items[0]] = vector


def CalcFather(wordlist, usedset):
        """
        Given wordlist and usedset, output the father word
        Compare each pair of word, calculate the similarity
        Use the most similar word to others as father
        """
        score = {}
        for word in wordlist:
                p = 0.0
                for other in wordlist:
                        sim = WordSimilarty(word, other)
                        p += sim
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

def CalcFatherv2(wordlist, usedset):
        """
        Given wordlist and usedset, output the father word
        Use the most frequency word as father.
        """
        score = {}
        for word in wordlist:
                if score.has_key(word):
                        score[word] += 1
                else:
                        score[word] = 1
        bestword = ""
        bestscore = 0
        for word in wordlist:
                if word in usedset:
                        continue
                if score[word] > bestscore or (score[word] == bestscore and len(word) < len(bestword)):
                        bestword = word
                        bestscore = score[word]
        return bestword


def GetWordVector(word):
        vec = [0 for dummy in range(len_vec)]
        for item in re.split('-|_|\s', word.lower().strip()):
                if word2vec.has_key(item):
                        itemvec = word2vec[item]
                        for idx in range(len_vec):
                                vec[idx] += itemvec[idx]
        return vec

def WordSimilarty_wordvec(wa, wb):
        veca = GetWordVector(wa)
        vecb = GetWordVector(wb)
        lena = lenb = dot = 0.0
        for item in veca:
                lena += item * item
        for item in vecb:
                lenb += item * item
        if lena == 0 or lenb == 0:
                return 0

        for idx in range(len_vec):
                dot += veca[idx] * vecb[idx]
        return dot / (math.sqrt(lena) * math.sqrt(lenb))

def IsSameWords_wordvec(wa, wb):
        sim = WordSimilarty_wordvec(wa, wb)
        if sim >= 0.45:
                return True
        else:
                return False

def WordSimilarty_editdis(wa, wb):
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

def IsSameWords_editdis(wa, wb):
        sim = (1 - WordSimilarty_editdis(wa, wb)) * max(len(wa), len(wb))
        if sim < 4.0:
                return True
        else:
                return False

def WordSimilarty(wa, wb):
        if SimiarityMethod == "EditDistance":
                return WordSimilarty_editdis(wa, wb)
        elif SimiarityMethod == "WordVector":
                return WordSimilarty_wordvec(wa, wb)
        else:
                return 0.0

def IsSameWords(wa, wb):
        if SimiarityMethod == "EditDistance":
                return IsSameWords_editdis(wa, wb)
        elif SimiarityMethod == "WordVector":
                return IsSameWords_wordvec(wa, wb)
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

#init_similarity()
#print GetWordVector("complex network")
# print WordSimilarty("cluster", "clusters")
# print WordSimilarty("cluster", "community")
# print WordSimilarty("cluster", "recommender")
#wordlist = ['cluster', 'clusters', 'clustering', 'community' ,'communities', 'recommender', 'recommendation', 'recommend']
#wordlist.append('recommend')
#print DuplicatedWords4ALL(wordlist)        
#print CalcFatherv2(wordlist, set())
#wordlist = ['cluster', 'clusters', 'clustering', 'community' ,'communities']
#print CalcFather(wordlist, set())
#wordlist = ['recommender', 'recommendation', 'recommend']
#print CalcFather(wordlist, set())

# fout = open('level1_father.txt', 'w')
# for line in file('../data/level1_lower.txt'):
#         items = line.strip().split(' ')
#         fout.write(CalcFather(items, set()) + '\n')
# fout.close()
