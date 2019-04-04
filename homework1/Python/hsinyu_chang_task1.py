import sys
import json
from pyspark import SparkContext, SparkConf, StorageLevel
from operator import add

#get arguments 
review = sys.argv[1]
outfile = sys.argv[2]

#construct configuration
conf = SparkConf()
conf.setMaster("local")
conf.setAppName("hw1")
sc = SparkContext.getOrCreate(conf)

#read jsonfile and parse it
reviews = sc.textFile(review)
reviewsRDD = reviews.map(lambda line: json.loads(line))\
.map(lambda review: (review['date'], review['user_id'], review['business_id'])).persist(StorageLevel.DISK_ONLY)  

#a
a = reviewsRDD.count()
#b
b = reviewsRDD.filter(lambda review: review[0][:4] == '2018').count()

#c, d
temp = reviewsRDD.map(lambda review: (review[1], 1)).reduceByKey(add).persist(StorageLevel.DISK_ONLY) 
c = temp.count()
d = temp.takeOrdered(10, key=lambda user: (-user[1], user[0]))
d = [list(r) for r in d]

temp.unpersist()

#e, f
temp = reviewsRDD.map(lambda review: (review[2], 1)).reduceByKey(add).persist(StorageLevel.DISK_ONLY) 
e = temp.count()
f = temp.takeOrdered(10, key=lambda business: (-business[1], business[0]))
f = [list(r) for r in f]

temp.unpersist()

#output dictionary
task1 = {
    "n_review" : a,
    "n_review_2018" : b,
    "n_user" : c,
    "top10_user": d,
    "n_business" : e,
    "top10_business": f
}

#write output json file
with open(outfile, 'w') as fd:
    json.dump(task1, fd)
