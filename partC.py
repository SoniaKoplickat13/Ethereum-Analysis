from mrjob.job import MRJob
from mrjob.step import MRStep

class partC(MRJob):
	def mapperC1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9:
				m = fields[2]
				s = fields[4]
				yield (m, int(s))

		except:
			pass

	def reducerC1(self, m, s):
		try:
			yield(m, sum(s))

		except:
			pass


	def mapperC2(s, m, totals):
		try:
			yield(None, (m,totals))
		except:
			pass

	def reducerC2(self, _, ms):
		j= 0
		try:
			sortsize = sorted(ms, reverse = True, key = lambda x:x[1])
			for k in sortsize[:10]:
				yield(k[0],k[1])
		except:
			pass


	def steps(self):
		return [MRStep(mapper = self.mapperC1, reducer=self.reducerC1), MRStep(mapper = self.mapperC2, reducer = self.reducerC2)]

if __name__ == '__main__':
	partC.run()
