from mrjob.job import MRJob
import re
import time
import statistics

class partA2(MRJob):

	def mapper(self,_, line):

		try:
			fields = line.split(',')
			if len(fields)==7:
				times = int(fields[6])
				diff = int(fields[5])
				months = time.strftime("%m", time.gmtime(times))
				years = time.strftime("%y", time.gmtime(times))
				yield((months,years),(diff,1))
		except:
			pass
	def reducerA(self, date, price):
		average = 0
		count = 0
		for s, k in price:
			average = (average*count+s*k)/(count + k)
			count = count +k
		return(date, (average,count))

	def combiner(self, date, price):
		yield self.reducerA(date,price)

	def reducer(self,date,price):
		date, (average,count) = self.reducerA(date,price)
		yield(date,average)

if __name__ == '__main__':
	partA2.run()
