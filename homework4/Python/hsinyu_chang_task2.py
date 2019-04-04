import sys
import os
from pyspark import SparkConf, SparkContext, StorageLevel, SQLContext
from itertools import combinations, groupby
from operator import itemgetter, add
from hsinyu_chang_Graph import Graph
import time
import os

#spark configuration
conf = SparkConf()
conf.setAppName('hw4')
conf.setMaster('local')
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
sc = SparkContext.getOrCreate(conf)

# arguments
threshold = int(sys.argv[1])
inputFilePath = sys.argv[2]
outputFilePathb = sys.argv[3]
outputFilePathc = sys.argv[4]

start = time.time()

inputFile = sc.textFile(inputFilePath)
header = inputFile.first()
#user_id,  business_id
inputRDD = inputFile.filter(lambda row: row!=header).map(lambda row: row.split(',')).map(lambda row: (row[0], row[1])).persist(StorageLevel.DISK_ONLY)

edgesRDD = inputRDD.map(lambda row: (row[1], row[0])).groupByKey().map(lambda row: sorted(list(row[1])))\
    .flatMap(lambda users: [((pair[0], pair[1]), 1) for pair in combinations(users, 2)]).reduceByKey(add)\
    .filter(lambda pair: pair[1] >= threshold).flatMap(lambda edge: [edge[0], (edge[0][1], edge[0][0])])

vertices = edgesRDD.map(lambda edge: edge[0]).distinct()

#adjacent
adjacentDict = dict(edgesRDD.groupByKey().mapValues(set).collect())

#
graph = Graph(adjacentDict)

#between
between = graph.Betweeness()
between = sorted(list(between.items()), key = lambda com: (-com[1], com[0]))
outputB = '\n'.join([str(edge) + ', ' + str(b) for (edge, b) in between])

with open(outputFilePathb, 'w') as fd:
    fd.write(outputB)
    fd.close()

#between
community = graph.detectCommunity()
community = [ sorted(list(com)) for com in community]
community.sort(key = lambda com: (len(com), com))
outputC = '\n'.join([str(com)[1:-1] for com in community])

print(graph.maxModularity)
print(len(graph.Community))

with open(outputFilePathc, 'w') as fd:
    fd.write(outputC)
    fd.close()

print("Duration: %d seconds" % (time.time()-start)) #18sec



