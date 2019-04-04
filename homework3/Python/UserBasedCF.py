# import sys
from pyspark import SparkConf, SparkContext, StorageLevel
from operator import add
from itertools import combinations 
# import time

def rateInterval(rate):
    if rate<0: return 0.0
    if rate>5: return 5.0
    return rate

def Valid2User(iterator, support, numofwhole):
    baskets = list(iterator)
    numofsub = len(baskets)
    sub_s = int(support*numofsub/numofwhole)
    tempcount = {}
        
    for basket in baskets:
        lst = sorted(list(basket), key = lambda t: t[0])
        for p1, p2 in combinations(lst, 2): 
            try: tempcount[tuple([p1[0], p2[0]])] += [(p1[1], p2[1])]
            except: tempcount[tuple([p1[0], p2[0]])] = [(p1[1], p2[1])]
    
    yield [(users, ratings) for users, ratings in tempcount.items() if len(ratings) >= sub_s]       

# all co-rated tuple ((u1, u2), (r1, r2)) on every items
def CartesianProduct(array):
    lst = sorted(list(array), key = lambda t: t[0])
    for p1, p2 in combinations(lst, 2): yield ((p1[0], p2[0]), (p1[1], p2[1]))
    # for p1, p2 in combinations(lst, 2): yield ((p1[0], p2[0]), [(p1[1], p2[1])])

#input > (('u1', 'u2'), [(4.0, 4.0), (5.0, 1.0)])
def Pearson(arrays):
    co_arrays = list(arrays)
    u1 = [r[0] for r in co_arrays]
    u2 = [r[1] for r in co_arrays]
    avg1, avg2 = sum(u1)/len(u1), sum(u2)/len(u2)
    up = sum([(u1[i]-avg1)*(u2[i]-avg2) for i in range(len(co_arrays))])
    left = sum([(u1[i]-avg1)**2 for i in range(len(co_arrays))])
    right = sum([(u2[i]-avg2)**2 for i in range(len(co_arrays))])

    if len(co_arrays) == 1: return 0    
    if left == 0 or right == 0: return 0   
    return up/left**0.5/right**0.5

def SplitSimilarity(sim):
    yield (sim[0][0], (sim[0][1], sim[1]))
    yield (sim[0][1], (sim[0][0], sim[1]))


def topSimilar(num, sim):
    sim = sorted(list(sim), key = lambda s: -s[1])
    return sim[:min(num, len(sim))]

def prediction(testpair, avgsDict, UserSimDict, UserDict, avgall):
    if not avgsDict.get(testpair[0]): return avgall
    if (not UserSimDict.get(testpair[0])) or len(UserSimDict[testpair[0]]) == 0: return avgsDict[testpair[0]]
    sim2Active = dict(UserSimDict[testpair[0]])
    up = [(UserDict[user][testpair[1]]-avgsDict[user])*weight for user, weight in sim2Active.items() if UserDict[user].get(testpair[1])]
    down = [abs(weight) for user, weight in sim2Active.items() if UserDict[user].get(testpair[1])]
    if len(down) == 0: return avgsDict[testpair[0]]
    else: return rateInterval(avgsDict[testpair[0]]+sum(up)/sum(down))

def UserBasedCF(trainRDD, testRDD):
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

    # find co-rated user couple
    trainRDD_item = trainRDD.map(lambda row: (row[1], (row[0], row[2]))).groupByKey().map(lambda row: row[1]).persist(StorageLevel.DISK_ONLY)
    numofitems = trainRDD_item.count()
    support = 12 #8>about 90 seconds 10>about 60 seconds #15 about 40 seconds #20>about 40 seconds

    # get Valid User Pairs ( with co-rated > support)
    UserSim = trainRDD_item.mapPartitions(lambda part: Valid2User(part, support, numofitems))\
        .flatMap(lambda x: x).reduceByKey(add)\
        .mapValues(Pearson).filter(lambda sim: abs(sim[1])>0).flatMap(lambda sim: SplitSimilarity(sim))\
        .groupByKey().map(lambda sim: (sim[0], topSimilar(3, sim[1]))).collect()

    UserSimDict = dict(UserSim)

    # prediction
    testPair = testRDD.map(lambda row: (row[0], row[1]))
    predictRate = testPair.map(lambda pair: (pair, prediction(pair, avgsDict, UserSimDict, UserDict, avgall))).persist(StorageLevel.DISK_ONLY)

    return predictRate























# len([len(user[1]) for user in UserSim])


# #group by items(features) > (u1, u2), (r1, r2) on same item > pearson
# CartesianProduct = trainRDD.map(lambda row: (row[1], (row[0], row[2]))).groupByKey().map(lambda row: row[1])\
#     .flatMap(lambda array: CartesianProduct(array))\
#     .persist(StorageLevel.DISK_ONLY)

# #Valid relationship
# #Valid = [(k, 'InValid') for k,v in CartesianProduct.countByKey().items() if v > 5]
# Valid = [ k for k, v in CartesianProduct.countByKey().items() if v > 5]

# UserSim = CartesianProduct.filter(lambda row: row[0] in Valid).groupByKey().persist(StorageLevel.DISK_ONLY).collect()\
#     .mapValues(Pearson).flatMap(lambda sim: SplitSimilarity(sim))\
#     .groupByKey().mapValues(list).persist(StorageLevel.DISK_ONLY).collect()

# test = CartesianProduct.filter(lambda row: row[0] in Valid).groupByKey().persist(StorageLevel.DISK_ONLY)

# len(Valid)
# ValidRDD = sc.parallelize(Valid)
# ValidCartesianProduct = CartesianProduct.subtractByKey(ValidRDD).groupByKey()\
#     .mapValues(Pearson).flatMap(lambda sim: SplitSimilarity(sim))\
#     .groupByKey().mapValues(list).collect()
    
#     .partitionBy(n_partition, lambda users: hash(users[0]) % n_partition)
    


# len(UserSim[0][1])

# UserSimDict = dict(UserSim)

# pair = ('u2', 'i2')
# UserSimDict[pair[0]]

# # #input> ('i1', (('u1', 4.0), ('u2', 4.0)))
# # def CartesianProduct(row):
# #     if row[1][0][0]<row[1][1][0]:
# #         return ((row[1][0][0], row[1][1][0]), (row[1][0][1], row[1][1][1]))
# #     if row[1][0][0]>row[1][1][0]:
# #         return ((row[1][1][0], row[1][0][0]), (row[1][1][1], row[1][0][1]))
    
    
    

# # t = trainRDD.map(lambda row: (row[1], (row[0], row[2]))).join(trainRDD.map(lambda row: (row[1], (row[0], row[2]))))\
# #     .filter(lambda row: row[1][0][0]!=row[1][1][0])\
# #     .map(CartesianProduct).collect()

# CartesianProduct = trainRDD.map(lambda row: (row[1], (row[0], row[2]))).groupByKey().map(lambda row: row[1])\
#     .flatMap(lambda array: CartesianProduct(array))\
#     .partitionBy(n_partition, lambda users: hash(users[0]) % n_partition)\
#     .persist(StorageLevel.DISK_ONLY)


# UserSim = CartesianProduct.groupByKey()\
#     .mapValues(Pearson).flatMap(lambda sim: SplitSimilarity(sim))\
#     .groupByKey().mapValues(list).collect()







# UserSim = trainRDD.map(lambda row: (row[1], (row[0], row[2]))).groupByKey().map(lambda row: row[1])\
#     .flatMap(lambda array: CartesianProduct(array)).groupByKey()\
#     .mapValues(Pearson).flatMap(lambda sim: SplitSimilarity(sim))\
#     .groupByKey().mapValues(list).collect()
















# # UserSim = trainRDD.map(lambda row: (row[1], (row[0], row[2]))).groupByKey().map(lambda row: row[1])\
# #     .flatMap(lambda array: CartesianProduct(array))\
# #     .aggregateByKey(((), ()), 
# #         lambda U, user: (U[0]+(user[0],), U[1]+(user[1],)), 
# #         lambda U, users: ((U[0]+users[0]), (U[1]+users[1])))\
# #     .mapValues(Pearson)


