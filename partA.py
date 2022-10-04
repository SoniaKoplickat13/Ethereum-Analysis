from mrjob.job import MRJob
import time
import re
import statistics

class partA(MRJob):
    def mapper(self, _,line):
        try:
            fields = line.split(',')
            if len(fields)  == 7:
                times = int(fields[6])
                months = time.strftime("%m", time.gmtime(times))
                year = time.strftime("%y", time.gmtime(times))
                yield ((months,year), 1)
        except:
            pass

    
        

    def reducer(self,word,counts):
        yield(word,sum(counts))

if __name__ == '__main__':
    partA.run()
