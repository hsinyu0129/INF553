# import sys
from pyspark import SparkConf, SparkContext, StorageLevel
from operator import add
from itertools import combinations 
# import time

def rateInterval(rate):
    if rate<0: return 0.0
    if rate>5: return 5.0
    return rate

def Valid2Item(iterator, support, numofwhole):
    baskets = list(iterator)
    numofsub = len(baskets)
    sub_s = int(support*numofsub/numofwhole)
    tempcount = {}
        
    for basket in baskets:
        lst = sorted(list(basket), key = lambda t: t[0])
        for p1, p2 in combinations(lst, 2): 
            try: tempcount[tuple([p1[0], p2[0]])] += [(p1[1], p2[1])]
            except: tempcount[tuple([p1[0], p2[0]])] = [(p1[1], p2[1])]
    
    yield [(items, ratings) for items, ratings in tempcount.items() if len(ratings) >= sub_s]       

# all co-rated tuple ((u1, u2), (r1, r2)) on every items
def CartesianProduct(array):
    lst = sorted(list(array), key = lambda t: t[0])
    for p1, p2 in combinations(lst, 2): yield ((p1[0], p2[0]), (p1[1], p2[1]))
    # for p1, p2 in combinations(lst, 2): yield ((p1[0], p2[0]), [(p1[1], p2[1])])

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
    if left == 0 or right == 0: return 0   
    return up/left**0.5/right**0.5

def SplitSimilarity(sim):
    yield (sim[0][0], (sim[0][1], sim[1]))
    yield (sim[0][1], (sim[0][0], sim[1]))


def topSimilar(num, sim):
    sim = sorted(list(sim), key = lambda s: -s[1])
    return sim[:min(num, len(sim))]

#prediction
def prediction(testpair, ItemSimDict, avgsDict, UserDict, avgall):
    if not avgsDict.get(testpair[0]): return avgall
    if (not ItemSimDict.get(testpair[1])) or len(ItemSimDict[testpair[1]]) == 0: return avgsDict[testpair[0]]
    
    ActiveU = UserDict[testpair[0]]
    sim2Active = dict(ItemSimDict[testpair[1]])
    items = [ item for item, sim in sim2Active.items()]

    # select users that have high similarity with active user and do prediction
    up = [ActiveU[item]*sim2Active[item] for item in items if ActiveU.get(item)]
    down = [abs(sim2Active[item]) for item in items if ActiveU.get(item)]
    if len(down)==0: return avgsDict[testpair[0]]
    return rateInterval(sum(up)/sum(down)) 

def ItemBasedCF(trainRDD, testRDD):
    #user, [(item1, r1), (item2, r2),....]
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

    # find co-rated item couple
    trainRDD_user = trainRDD.map(lambda row: (row[0], (row[1], row[2]))).groupByKey().map(lambda row: row[1]).persist(StorageLevel.DISK_ONLY)
    numofuser = trainRDD_user.count()
    support = 7 #8>about 90 seconds

    #get Valid User Pairs ( with co-rated > support)
    ItemSim = trainRDD_user.mapPartitions(lambda part: Valid2Item(part, support, numofuser))\
        .flatMap(lambda x: x).reduceByKey(add)\
        .mapValues(Pearson).filter(lambda sim: abs(sim[1])>0).flatMap(lambda sim: SplitSimilarity(sim))\
        .groupByKey().persist(StorageLevel.DISK_ONLY)
    ItemSim2 = ItemSim.map(lambda sim: (sim[0], topSimilar(2, sim[1]))).collect()

    ItemSimDict = dict(ItemSim2)

    # prediction
    testPair = testRDD.map(lambda row: (row[0], row[1]))
    predictRate = testPair.map(lambda pair: (pair, prediction(pair, ItemSimDict, avgsDict, UserDict, avgall))).persist(StorageLevel.DISK_ONLY)
    return predictRate










