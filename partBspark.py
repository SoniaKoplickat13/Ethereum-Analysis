import pyspark
import re

sc = pyspark.SparkContext()

def transactions_part(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        return False

def contracts_part(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        return False

a_con = sc.textFile('hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts')
ab_con = a_con.filter(contracts_part)
address = ab_con.map(lambda l: (l.split(',')[0], 1))

a_tra = sc.textFile('hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions')
ab_tra = a_tra.filter(transactions_part)
address_val_pair = ab_tra.map(lambda l: (l.split(',')[2], float(l.split(',')[3])))
results = address_val_pair.join(address)
address_val_pair_agg = results.reduceByKey(lambda (a,b),(c,d): (float(a) + float(c), b+d))

top10 = address_val_pair_agg.takeOrdered(10, key = lambda x: -x[1][0])

for rec in top10:
    print('{},{}'.format(rec[0], rec[1][0]))
