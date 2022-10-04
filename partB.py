from mrjob.job import MRJob
from mrjob.step import MRStep

class partB(MRJob):
	def mapperB1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				address = fields[2]
				value = int(fields[3])
				yield address, (1,value)
			elif len(fields) == 5:
				address1 = fields[0]
				yield address1, (2,1)
		except:
			pass
	def reducerB1(self, key, values):
		f = False
		allv = []
		for m in values:
			if m[0]==1:
				allv.append(m[1])
			elif m[0] == 2:
				f = True
		if f:
			yield key, sum(allv)

	def mapperB2(self, key,value):
		yield None, (key,value)

	def reducerB2(self, _, keys):
		sortedv = sorted(keys, reverse = True, key = lambda x: x[1])
		for i in sortedv[:10]:
			yield i[0], i[1]

	def steps(self):
		return [MRStep(mapper = self.mapperB1, reducer=self.reducerB1), MRStep(mapper = self.mapperB2, reducer = self.reducerB2)]

if __name__ == '__main__':
	partB.run()
