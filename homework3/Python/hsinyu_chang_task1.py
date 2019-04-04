import sys
from pyspark import SparkConf, SparkContext, StorageLevel
from random import *
import itertools
from operator import add
import time

def isPrime(num):
    for m in range(2, int(num**0.5)+1):
        if not num%m:
            return False
    return True

def findPrime(num):
    for n in range(num+1, num+10000):
        if isPrime(n): return n

# hash > f(x) = ((ax + b) % p) % m:
def HashParaGenerator(num, bin):
    a = sample(range(1,num*100), num)
    b = sample(range(1,num*100), num)
    p = [findPrime(i) for i in sample(range(1,num*100), num)]

    return {'a':a, 'b':b, 'p':p ,'m':findPrime(bin)} 

def hashValue(row, hpara):
    # print(hpara)
    a = hpara['a']
    b = hpara['b']
    p = hpara['p']
    m = hpara['m']

    hashlength = len(a)

    for h in range(hashlength):
        #((business_id, hashindex), hashvalue)
        yield ((row[0], h),(((row[1]*a[h]+b[h])%p[h])%m))

def Jarccard(pair, inputGroup):
    set1 = inputGroup[pair[0]]
    set2 = inputGroup[pair[1]]
    return (pair, len(set1&set2)/len(set1|set2))

start = time.time()

#spark configuration
conf = SparkConf()
conf.setAppName('hw3')
conf.setMaster('local')
sc = SparkContext.getOrCreate(conf)

#arguments
inputFilePath = sys.argv[1]
outputFilePath = sys.argv[2]

# inputFilePath = '/Users/changhsinyu/Desktop/hsinyu_chang_hw3/yelp_train.csv'
# outputFilePath = '/Users/changhsinyu/Desktop/hsinyu_chang_hw3/output/python_hsinyu_chang_task1.csv'

inputFile = sc.textFile(inputFilePath)
header = inputFile.first()
#user_id,  business_id
inputRDD = inputFile.filter(lambda row: row!=header).map(lambda row: row.split(',')).map(lambda row: (row[0], row[1])).persist(StorageLevel.DISK_ONLY)

#all business
#businessList = inputRDD.map(lambda row: row[1]).distinct().sortBy(lambda business: business).collect()

#all users:
userList = inputRDD.map(lambda row: row[0]).distinct().sortBy(lambda user: user)
userIndex = dict([(u, index) for (index, u) in enumerate(userList.collect())])
userBin = userList.count()

#indexing user:
#(business_id, user_Index)
inputIndex = inputRDD.map(lambda row: (row[1], userIndex[row[0]])).persist(StorageLevel.DISK_ONLY)

#grouping by business: every pair needs to group
inputGroup = dict(inputIndex.groupByKey().mapValues(lambda row: set(row)).collect())

#number of minhash 
minhash_num = 51
hash_para = HashParaGenerator(minhash_num, userBin)
# r=2/b=12/recall=0.99  r=3/b=16/recall=0.89
bands = 18

#minhash
signs = inputIndex.flatMap(lambda row: hashValue(row, hash_para)).reduceByKey(lambda x, y: y if x >= y else x).persist(StorageLevel.DISK_ONLY)
# output > ((business_id, hashindex), minhashvalue)

signs_bands = signs.map(lambda row: ((row[0][1]%bands, row[0][0]), (row[0][1], row[1]))).groupByKey()\
    .map(lambda row: (frozenset(row[1]), row[0][1])).groupByKey().map(lambda business: sorted(list(business[1])))\
    .filter(lambda bucket: len(bucket)>1).persist(StorageLevel.DISK_ONLY)
# output > (bandIndex, business_id), (hashIndex, hashValue)
# output > (frozenset(hashIndex, hashValue)), business_id
# output > business_ids

candidate_pairs = signs_bands.flatMap(lambda bucket: [pair for pair in itertools.combinations(bucket, 2)]).distinct().sortBy(lambda pair: (pair[0], pair[1])).persist(StorageLevel.DISK_ONLY)

# Jaccard similarity for every candidate pairs from LSH
PairsSim = candidate_pairs.map(lambda pair: Jarccard(pair, inputGroup)).filter(lambda sim: sim[1]>=0.5).sortBy(lambda pair: (pair[0][0], pair[0][1])).persist(StorageLevel.DISK_ONLY)

#write txt file
header = sc.parallelize(["business_id_1, business_id_2, similarity"],1)
PairsSimOutput = PairsSim.map(lambda sim: sim[0][0]+','+sim[0][1]+','+str(sim[1]))
output = '\n'.join(header.union(PairsSimOutput).collect())
with open(outputFilePath, 'w') as fd:
    fd.write(output)
    fd.close()

end = time.time()
print("Duration: %d seconds" % (end-start))


# Pairs = PairsSim.map(lambda sim: sim[0])
# truth = sc.textFile('/Users/changhsinyu/Desktop/hsinyu_chang_hw3/pure_jaccard_similarity.csv')
# header1 = truth.first()
# #user_id,  business_id
# truth_pairs = truth.filter(lambda row: row!=header1).map(lambda row: row.split(','))\
#     .map(lambda row: (row[0], row[1])).persist(StorageLevel.DISK_ONLY)

# tp = Pairs.intersection(truth_pairs).count()
# fp = Pairs.subtract(truth_pairs).count()
# fn = truth_pairs.subtract(Pairs).count()
# print('precision: %0.5f' % (float(tp)/(tp+fp)))
# print('recall: %0.5f' % (float(tp)/(tp+fn)))
# print("Duration: %d seconds" % (end-start))

# precision = float(tp)/(tp+fp)
# recall = float(tp)/(tp+fn)


