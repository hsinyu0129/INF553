import sys
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

from graphframes import *
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext, SparkSession

from itertools import combinations
from operator import add
import time

#spark configuration
conf = SparkConf()
conf.setAppName('hw4')
conf.setMaster('local')
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
conf.set("spark.sql.shuffle.partitions", "4")

sc = SparkContext.getOrCreate(conf)
sqlContext = SQLContext(sc)

# arguments
threshold = int(sys.argv[1])
inputFilePath = sys.argv[2]
outputFilePath = sys.argv[3]

start = time.time()
inputFile = sc.textFile(inputFilePath)
header = inputFile.first()

#user_id,  business_id
inputRDD = inputFile.filter(lambda row: row!=header).map(lambda row: row.split(',')).map(lambda row: (row[0], row[1])).persist(StorageLevel.DISK_ONLY)

edgesRDD = inputRDD.map(lambda row: (row[1], row[0])).groupByKey().map(lambda row: sorted(list(row[1])))\
    .flatMap(lambda users: [((pair[0], pair[1]), 1) for pair in combinations(users, 2)]).reduceByKey(add)\
    .filter(lambda pair: pair[1] >= threshold).flatMap(lambda edge: [edge[0], (edge[0][1], edge[0][0])])

vertices = edgesRDD.map(lambda edge: edge[0]).distinct().zipWithIndex().map(lambda user: (str(user[1]), user[0])).toDF(['id', 'user_id']).persist(StorageLevel.DISK_ONLY)
# edges = edgesRDD.toDF(['src', 'dst'])

# (user_id, id) > user_id: id
verticesDict = dict(vertices.rdd.map(lambda row: (row[1], row[0])).collect())

# (id1, id2)
# indexing
edgesArray = edgesRDD.map(lambda edges: (verticesDict[edges[0]], verticesDict[edges[1]])).collect()
edges = sqlContext.createDataFrame(edgesArray, ["src", "dst"]).persist(StorageLevel.DISK_ONLY)

gragh = GraphFrame(vertices, edges)
# id, user_id, label
communities = gragh.labelPropagation(maxIter=5)

communitiesRDD = communities.rdd.map(lambda row: (row[2], row[1])).groupByKey().map(lambda com: sorted(list(com[1]))).sortBy(lambda com: (len(com), com))
output = '\n'.join(communitiesRDD.map(lambda com: "'" + "', '".join(com) + "'").collect())

with open(outputFilePath, 'w') as fd:
    fd.write(output)
    fd.close()

end = time.time()
print("Duration: %d seconds" % (end-start))


# cogrouped = rdd1.cogroup(rdd2)
# cogrouped.mapValues(lambda x: (list(x[0]), list(x[1]))).collect()
# ## [('foo', ([1], [4])), ('bar', ([2], [5, 6])), ('baz', ([3], []))]








