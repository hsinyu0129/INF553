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

def CartesianProduct(array, pairs):
    # lst = sorted(list(array), key = lambda t: t[0])
    # for p1, p2 in itertools.combinations(lst, 2): 
    #     if (p1[0],p2[0]) in pairs: yield ((p1[0], p2[0]), (p1[1], p2[1]))
    dic = dict(list(array))
    for pair in pairs:
        if dic.get(pair[0]) and dic.get(pair[1]): yield (pair, (dic[pair[0]], dic[pair[1]]))

#input > (('i1', 'i2'), [(4.0, 4.0), (5.0, 1.0)])
def Pearson(arrays):
    co_arrays = list(arrays)
    i1 = [r[0] for r in co_arrays]
    i2 = [r[1] for r in co_arrays]
    avg1, avg2 = sum(i1)/len(i1), sum(i2)/len(i2)
    up = sum([(i1[i]-avg1)*(i2[i]-avg2) for i in range(len(co_arrays))])
    left = sum([(i1[i]-avg1)**2 for i in range(len(co_arrays))])
    right = sum([(i2[i]-avg2)**2 for i in range(len(co_arrays))])

    if len(co_arrays) == 1: return 0 
    # (5, 4, 5)  (5, 5, 5) 
    if left == 0 or right == 0: return 0   
    return up/left**0.5/right**0.5

def SplitSimilarity(sim):
    yield (sim[0][0], (sim[0][1], sim[1]))
    yield (sim[0][1], (sim[0][0], sim[1]))

def prediction(testpair, ItemSimDict, avgsDict, UserDict, avgall):
    if not avgsDict.get(testpair[0]): return avgall
    if (not ItemSimDict.get(testpair[1])) or len(ItemSimDict[testpair[1]]) == 0: return avgsDict[testpair[0]]
    
    ActiveU = UserDict[testpair[0]]
    sim2Active = dict(ItemSimDict[testpair[1]])
    items = [ item for item, sim in sim2Active.items()]

    # select users that have high similarity with active user and do prediction
    up = [ActiveU[item]*sim2Active[item] for item in items if ActiveU.get(item)]
    down = [abs(sim2Active[item]) for item in items if ActiveU.get(item)]
    if len(down)==0 or sum(down) == 0: return avgsDict[testpair[0]]
    return rateInterval(sum(up)/sum(down)) 

def rateInterval(rate):
    if rate<0: return 0.0
    if rate>5: return 5.0
    return rate

def ItemBasedCFwLSH(trainRDD, testRDD):
    
    trainRDD_user = trainRDD.map(lambda row: (row[0], (row[1], row[2]))).groupByKey().mapValues(dict).collect()
    UserDict = dict(trainRDD_user)

    #user, avg
    avgs = trainRDD.map(lambda row: (row[0], row[2]))\
        .aggregateByKey((0,0), 
        lambda U, rating: (U[0]+rating, U[1]+1), 
        lambda U, rating: (U[0]+rating[0], U[1]+rating[1]))\
        .map(lambda row: (row[0], row[1][0]/row[1][1])).collect()

    avgsDict = dict(avgs)
    avgall = trainRDD.map(lambda row: row[2]).mean()

    ## LSH step
    #user_id,  business_id
    inputRDD = trainRDD.map(lambda row: (row[0], row[1])).persist(StorageLevel.DISK_ONLY)

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
    minhash_num = 36
    hash_para = HashParaGenerator(minhash_num, userBin)
    # r=2/b=12/recall=0.99  r=3/b=16/recall=0.89
    bands = 13

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
    PairsSim = candidate_pairs.map(lambda pair: Jarccard(pair, inputGroup)).filter(lambda sim: sim[1]>=0.2).persist(StorageLevel.DISK_ONLY)
    pairs = [pair[0] for pair in PairsSim.collect()]

    #group by users(features) > (i1, i2), (r1, r2) from same user > pearson
    ItemProduct = trainRDD.map(lambda row: (row[0], (row[1], row[2]))).groupByKey().map(lambda row: row[1])\
        .flatMap(lambda array: CartesianProduct(array, pairs))
    ItemSim = ItemProduct.groupByKey()\
        .mapValues(Pearson).flatMap(lambda sim: SplitSimilarity(sim))\
        .groupByKey().mapValues(list).collect()

    ItemSimDict = dict(ItemSim)

    # prediction
    testPair = testRDD.map(lambda row: (row[0], row[1]))
    predictRate = testPair.map(lambda pair: (pair, prediction(pair, ItemSimDict, avgsDict, UserDict, avgall))).persist(StorageLevel.DISK_ONLY)
    return predictRate

