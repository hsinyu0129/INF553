import sys
import json
from pyspark import SparkContext, SparkConf, StorageLevel
from operator import add

#get arguments 
review = sys.argv[1]
business = sys.argv[2]
outfileA = sys.argv[3]
outfileB = sys.argv[4]

#construct configuration
conf = SparkConf()
conf.setMaster("local")
conf.setAppName("hw1")
sc = SparkContext.getOrCreate(conf)

#read review jsonfile and parse it
reviews = sc.textFile(review)
reviewsRDD = reviews.map(lambda line: json.loads(line))\
.map(lambda review: (review['business_id'], float(review['stars'])))

#read business jsonfile and parse it
businesss = sc.textFile(business)
businessRDD = businesss.map(lambda line: json.loads(line))\
.map(lambda business: (business['business_id'], business['city']))

#join two RDD with business_id
a = businessRDD.join(reviewsRDD).map(lambda b: b[1])\
.aggregateByKey((0,0), lambda U, star: (U[0]+star, U[1]+1), lambda U, star: (U[0]+star[0], U[1]+star[1]))\
.map(lambda city: (city[0], city[1][0]/city[1][1]))\
.sortBy(lambda city: (-city[1], city[0])).persist(StorageLevel.DISK_ONLY)

#write txt file
header = sc.parallelize(["city,stars"],1)
A = a.map(lambda t: t[0]+','+str(t[1]))
header.union(A).coalesce(1).saveAsTextFile(outfileA)

#B

import time
#method1
start = time.time()
collectB = a.collect()
for city in collectB[:10]:   
    print('%s,%0.1f' % (city[0], city[1]))

end = time.time()
m1 = end-start

#method2
start = time.time()
for city in a.take(10):
    print('%s,%0.1f' % (city[0], city[1]))
end = time.time()
m2 = end-start

#task3B output dictionary
task3B = {
    "m1" : m1,
    "m2" : m2,
    "explanation": "In method1, all pairs are collected first, even though most of the pairs are not going to be used in the 'Printing' process. However, method2 just takes the first 10 pairs and stop. In sum, method2 is faster than method1."
}

#write output json file
with open(outfileB, 'w') as fd:
    json.dump(task3B, fd)

