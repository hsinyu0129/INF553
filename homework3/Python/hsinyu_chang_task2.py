import sys
from pyspark import SparkConf, SparkContext, StorageLevel
from operator import add
from ModelBasedCF import ModelBasedCF
from UserBasedCF import UserBasedCF
from ItemBasedCF import ItemBasedCF
from ItemBasedCFwLSH import ItemBasedCFwLSH
import time

#spark configuration
conf = SparkConf()
conf.setMaster("local")
conf.setAppName("hw3")
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
sc = SparkContext.getOrCreate(conf)

start = time.time()

#arguments
trainFilePath = sys.argv[1]
testFilePath = sys.argv[2]
case = sys.argv[3]
outputFilePath = sys.argv[4]

# trainFilePath = '/Users/changhsinyu/Desktop/hsinyu_chang_hw3/yelp_train.csv'
# testFilePath = '/Users/changhsinyu/Desktop/hsinyu_chang_hw3/yelp_val.csv'

#train
trainFile = sc.textFile(trainFilePath,3)
train_header = trainFile.first()
#user_id,  business_id, ratings
trainRDD = trainFile.filter(lambda row: row!=train_header).map(lambda row: row.split(','))\
    .map(lambda row: tuple([row[0], row[1], float(row[2])])).persist(StorageLevel.DISK_ONLY)

#test
testFile = sc.textFile(testFilePath,2)
test_header = testFile.first()
#user_id,  business_id, ratings
testRDD = testFile .filter(lambda row: row!=test_header).map(lambda row: row.split(','))\
    .map(lambda row: tuple([row[0], row[1], float(row[2])]))

if case == '1': predictRate = ModelBasedCF(trainRDD, testRDD) # 1.126 32sec numlambda, rank= 10, 0.05, 3
if case == '2': predictRate = UserBasedCF(trainRDD, testRDD) # 1.113 80sec top3
if case == '3': predictRate = ItemBasedCF(trainRDD, testRDD) # 1.156 112sec top2
if case == '4': predictRate = ItemBasedCFwLSH(trainRDD, testRDD) # 1.156 112sec top2   

# write file
header = sc.parallelize(["user_id, business_id, prediction"],1)
predictRateOutput = predictRate.map(lambda pred: pred[0][0]+','+pred[0][1]+','+str(pred[1]))
output = '\n'.join(header.union(predictRateOutput).collect())
with open(outputFilePath, 'w') as fd:
    fd.write(output)
    fd.close()

end = time.time()
print('Duration: %d' % (end-start))

# #Evaluation
# evalu = testRDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictRate).persist(StorageLevel.DISK_ONLY)
# distribution = evalu.map(lambda r: abs(r[1][0] - r[1][1])).persist(StorageLevel.DISK_ONLY)
# MSE = evalu.map(lambda r: (r[1][0] - r[1][1])**2).mean()

# diff1 = distribution.filter(lambda diff: 0 <= diff < 1).count()
# diff2 = distribution.filter(lambda diff: 1 <= diff < 2).count()
# diff3 = distribution.filter(lambda diff: 2 <= diff < 3).count()
# diff4 = distribution.filter(lambda diff: 3 <= diff < 4).count()
# diff4 = distribution.filter(lambda diff: 3 <= diff < 4).count()
# diff5 = distribution.filter(lambda diff: 4 <= diff).count()


# print(">=0 and <1: %d" % (diff1))
# print(">=1 and <2: %d" % (diff2))
# print(">=2 and <3: %d" % (diff3))
# print(">=3 and <4: %d" % (diff4))
# print(">=4: %d" % (diff5))
# print("RMSE: %0.3f" % MSE**0.5)
# print('Duration: %d' % (end-start))