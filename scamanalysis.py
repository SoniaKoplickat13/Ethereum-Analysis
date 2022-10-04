from mrjob.job import MRJob
from mrjob.step import MRStep


class partDscams(MRJob):
    def mapper1(self, _, lines):
        try:
            fields = lines.split(",")
            # transactions
            if len(fields) == 7:
                address = fields[2]
                value = float(fields[3])
                yield (address, (1,value,None))
            #scams
            if len(fields) == 3:
                address = fields[0]
                category = fields[1]
                status = fields[2]
                yield(address, (2, category, status))

        except:
            pass

    def reducer1(self, key, values):
        transaction = []
        category = []
        status = []
        for k in values:
            m = k[0]
            # transactions
            if m == 1:
                transaction.append(k[1])
            # scams
            elif m == 2:
                category.append(k[1])
                status.append(k[2])

        if len(transaction) >0  and len(status) > 0:
            yield ((status[0],category[0]), sum(transaction))

    
    def reducer2(self, key, value):
        yield(key,sum(value))

    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(reducer = self.reducer2)]

if __name__ == '__main__':
    partDscams.run()
