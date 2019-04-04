import sys
from pyspark import SparkContext, SparkConf, StorageLevel
import itertools
from itertools import groupby
from operator import add 
import time

def Union(lst1, lst2): 
    #combination of two list
    return sorted(list(set(lst1) | set(lst2)))

def intersection(lst1, lst2): 
    return sorted(list(set(lst1) & set(lst2)))

def SizekCandidate(preFreq, k): #preFreq> list of lists 
    candidates = []
    #do merge 2 k-1 frequent which produce a size k candidate > not following apriori rule
    for index, f1 in enumerate(preFreq[:-1]):
        for f2 in preFreq[index+1:]:
            if f1[:-1] == f2[:-1]:
                candidates.append(tuple(Union(f1, f2)))
            if f1[:-1] != f2[:-1]:
                break
                
    return candidates 

def freqinSubset(iterator, distinctItems, support, numofwhole):
    baskets = list(iterator)
    numofsub = len(baskets)
    sub_s = int(support*numofsub/numofwhole)
    
    freqSub = {}
    candies = [tuple([x]) for x in distinctItems] #single candidate
    k=0

    while candies:
        k+=1
        tempcount = {}
        for basket in baskets:
            if len(basket)>=k:
                for candy in candies:
                    if set(candy).issubset(set(basket)):
                        try: tempcount[candy] +=1
                        except: tempcount[candy] =1

        freqSub[k] = sorted([items for items, count in tempcount.items() if count >= sub_s])        
        candies = SizekCandidate(freqSub[k], k+1) 
    
    allsubFreq = []
    for _, candy in freqSub.items():
        allsubFreq.extend(candy)
    
    yield allsubFreq


def countOnWhole(basket, candidates):
    result = []
    for candy in candidates:
        if set(candy).issubset(set(basket)):
        	result.extend([(tuple(candy),1)])
    yield result

def groupByLength(ItemsSets):
    dic = {}
    for key, group in itertools.groupby(ItemsSets, lambda items: len(items)):
        dic[key] = sorted(list(group), key=lambda x : x) 
    
    return dic

def strItemSets(ItemSets):
    ItemSetsD = groupByLength(ItemSets)
    output = ''
    for _, items in ItemSetsD.items():
        output += str([list(x) for x in items])[1:-1].replace('[', '(').replace(' (', '(').replace(']', ')')+'\n\n'
    return output[:-2]

#main function######### 
 
start = time.time()
    
#arguments
case = sys.argv[1]
support = int(sys.argv[2])
inputFilePath = sys.argv[3]
outputFilePath = sys.argv[4]

conf = SparkConf()
conf.setMaster("local")
conf.setAppName("hw2")
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
sc = SparkContext.getOrCreate(conf)

inputFile = sc.textFile(inputFilePath, 2)
header = inputFile.first()

if case == '1': #group by users
    inputRDD = inputFile.filter(lambda line: line!= header).map(lambda line: line.split(',')).map(lambda t:tuple(t))\
        .groupByKey().mapValues(set).mapValues(sorted).mapValues(tuple).map(lambda t: (1,t[1])).persist(StorageLevel.DISK_ONLY)
elif case == '2': #group by business_ids
    inputRDD = inputFile.filter(lambda line: line!= header).map(lambda line: line.split(',')).map(lambda t:(t[1], t[0]))\
        .groupByKey().mapValues(set).mapValues(sorted).mapValues(tuple).map(lambda t: (1,t[1])).persist(StorageLevel.DISK_ONLY)

#count distinct items
distinctItems = inputRDD.flatMapValues(tuple)\
    .map(lambda t : (t[1],t[0])).groupByKey()\
    .map(lambda t : (t[0])).collect()
distinctItems.sort()


#whole baskets number
whole_number = inputRDD.count()
baskets = inputRDD.map(lambda t: t[1]).persist(StorageLevel.DISK_ONLY)

#first phase > find candidate in subsets
FreqinSample = baskets.mapPartitions(lambda part:freqinSubset(part, distinctItems, support, whole_number))\
            .flatMap(lambda x: x).distinct().sortBy(lambda t: (len(t), t)).collect()


#2nd phase > count on whole
frequentI = baskets.flatMap(lambda basket: countOnWhole(basket, FreqinSample)).flatMap(lambda x: x).reduceByKey(add)\
    .filter(lambda items: items[1] >= support)\
    .map(lambda items:items[0]).sortBy(lambda t: (len(t), t)).collect()

with open(outputFilePath, 'w') as fd:
    output = 'Candidates:\n' + strItemSets(FreqinSample)+'\n\n'+'Frequent Itemsets:\n' + strItemSets(frequentI)
    fd.write(output)
    fd.close()

print("Duration: %d seconds" % (time.time()-start))
