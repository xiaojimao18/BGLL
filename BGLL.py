import os

class BGLL:
	convert_exe   = ''
	community_exe = ''
	hierarchy_exe = ''

	source_file = ''
	bin_file    = ''
	tree_file   = ''

	is_weighted = False		# graph with weight

	def __init__(self, base_dir, is_weighted):
		self.convert_exe   = './BGLL/convert'
		self.community_exe = './BGLL/community'
		self.hierarchy_exe = './BGLL/hierarchy'

		self.source_file = base_dir + 'bgll.txt'
		self.bin_file    = base_dir + 'bgll.bin'
		self.tree_file   = base_dir + 'bgll.tree'

		self.is_weighted = is_weighted


	def callBGLL(self):
		if self.is_weighted:
			# graph with weight
			os.system("%s -i %s -o %s -w" % (self.convert_exe, self.source_file, self.bin_file))
			os.system("%s %s -w -l -1 > %s" % (self.community_exe, self.bin_file, self.tree_file))
		else:
			# graph without weight
			os.system("%s -i %s -o %s" % (self.convert_exe, self.source_file, self.bin_file))
			os.system("%s %s -l -1 > %s" % (self.community_exe, self.bin_file, self.tree_file))

	def get_total_level(self):
		output = os.popen("%s %s -n" % (self.hierarchy_exe, self.tree_file))
		return len(output.readlines()) - 2

	def get_level_output(self, level):
		return os.popen("%s %s -l %d" % (self.hierarchy_exe, self.tree_file, level))
