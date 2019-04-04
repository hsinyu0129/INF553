import sys
import json
from pyspark import SparkContext, SparkConf, StorageLevel
from operator import add
import time

def countPartition(iterator):
  yield sum(1 for _ in iterator)

review = sys.argv[1]
outfile = sys.argv[2]
n_partition = int(sys.argv[3])

conf = SparkConf()
conf.setMaster("local")
conf.setAppName("hw1")
sc = SparkContext.getOrCreate(conf)

reviews = sc.textFile(review)
reviewsRDD = reviews.map(lambda line: json.loads(line)).map(lambda review: (review['business_id'], 1)).persist(StorageLevel.DISK_ONLY) 

#default
default_partition = reviewsRDD.getNumPartitions()
default_n_items = reviewsRDD.mapPartitions(countPartition).collect()

defaultStart = time.time()
defaultF = reviewsRDD.reduceByKey(add).takeOrdered(10, key=lambda business: (-business[1], business[0]))
defaultEnd = time.time()
m1 = defaultEnd-defaultStart


#customized

customizedRDD = reviewsRDD.partitionBy(n_partition, lambda business: hash(business) % n_partition).persist(StorageLevel.DISK_ONLY) 
customized_n_items = customizedRDD.mapPartitions(countPartition).collect()
customizedStart = time.time()
customizedF = customizedRDD.reduceByKey(add).takeOrdered(10, key=lambda business: (-business[1], business[0]))
customizedEnd = time.time()
m2 = customizedEnd-customizedStart



task2 = {
    "default":{
        "n_partition": default_partition,
        "n_items": default_n_items,
        "exe_time": defaultEnd - defaultStart

    },
    "customized":{
        "n_partition": n_partition,
        "n_items": customized_n_items,
        "exe_time": customizedEnd-customizedStart
    },
    "explanation": "If a proper number of partitions is chosen, using a customized partitioner, hash(business_id) % n_partition, will send items with same business_id into the same bucket, therefore we can reduce the shuffling costs when we need to operate reduceBy() on the 'business_id' key and save time."
}

with open(outfile, 'w') as fd:
    json.dump(task2, fd)
