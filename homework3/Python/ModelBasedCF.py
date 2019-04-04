from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

def rateInterval(rate):
    if rate<0: return 0.0
    if rate>5: return 5.0
    return rate 

def ModelBasedCF(trainRDD, testRDD):

    #indexing 6s
    users = trainRDD.map(lambda row: row[0]).union(testRDD.map(lambda row: row[0])).distinct().zipWithUniqueId().collect()
    userIndex = dict(users)
    indexUser = {v:k for k,v in userIndex.items()}

    businesss = trainRDD.map(lambda row: row[1]).union(testRDD.map(lambda row: row[1])).distinct().zipWithUniqueId().collect()
    businessIndex = dict(businesss)
    indexBusiness = {v:k for k,v in businessIndex.items()}


    #user, avg 1s
    avgs = trainRDD.map(lambda row: (row[0], row[2]))\
        .aggregateByKey((0,0), 
        lambda U, rating: (U[0]+rating, U[1]+1), 
        lambda U, rating: (U[0]+rating[0], U[1]+rating[1]))\
        .map(lambda row: (userIndex[row[0]], row[1][0]/row[1][1])).collect()
    avgsDict = dict(avgs)
    avgall = trainRDD.map(lambda row: row[2]).mean()

    #train data
    trainRating = trainRDD.map(lambda row: Rating(userIndex[row[0]], businessIndex[row[1]], row[2]))

    #test data with indexing
    testPair = testRDD.map(lambda row: (userIndex[row[0]], businessIndex[row[1]]))

    #cross_validation CV=3 > parameter tuning
    #  > rank
    # {'numIterations': 10, 'numlambda': 0.01, 'rank': 2} : 1.28 (more stable)
    # {'numIterations': 10, 'numlambda': 0.03, 'rank': 2} : 1.23
    # {'numIterations': 10, 'numlambda': 0.05, 'rank': 2} : 1.20^, 1.12(val)
    # {'numIterations': 10, 'numlambda': 0.01, 'rank': 3} : 1.32
    # {'numIterations': 10, 'numlambda': 0.03, 'rank': 3} : 1.28 (less stable)
    # {'numIterations': 10, 'numlambda': 0.05, 'rank': 3} : 1.23^, 1.15(val)
    # numIterations, numlambda, rank= 10, 0.05, 3
    numIterations, numlambda, rank= 10, 0.05, 3

    #  > lambda
    # {'numIterations': 10, 'numlambda': 0.05, 'rank': 10} : 1.36
    # {'numIterations': 10, 'numlambda': 0.05, 'rank': 15} : 1.34
    # {'numIterations': 10, 'numlambda': 0.05, 'rank': 20} : 1.31
    # {'numIterations': 10, 'numlambda': 0.07, 'rank': 10} : 1.28
    # {'numIterations': 10, 'numlambda': 0.07, 'rank': 15} : 1.26
    # {'numIterations': 10, 'numlambda': 0.07, 'rank': 20} : 1.24^, 1.17(val)
    # numIterations, numlambda, rank= 10, 0.07, 20 
    #modeling 9s
    model = ALS.train(trainRating, rank, numIterations, numlambda)

    #prediction
    predictSub = model.predictAll(testPair).map(lambda r: ((r[0], r[1]), rateInterval(r[2])))
    predictAvg = testPair.map(lambda pair: (pair, None)).subtractByKey(predictSub)\
        .map(lambda pair: ((indexUser[pair[0][0]], indexBusiness[pair[0][1]]), rateInterval(avgsDict[pair[0][0]])) if avgsDict.get(pair[0][0]) else ((indexUser[pair[0][0]], indexBusiness[pair[0][1]]), avgall))
    predictRate = predictSub.map(lambda r: ((indexUser[r[0][0]], indexBusiness[r[0][1]]), rateInterval(r[1]))).union(predictAvg)

    return predictRate


    #cross validation for model-based:
    # from sklearn.model_selection import ParameterGrid
    # import numpy
    # from numpy.random import choice
    
    # CV = 3
    # param_grid = {'rank': [2, 3], 'numIterations': [10], 'numlambda': [0.01, 0.03, 0.05]}
    # param_grid = {'rank': [10, 15, 20], 'numIterations': [10], 'numlambda': [0.03, 0.05, 0.07]}
    
    # trainCV = trainRDD.map(lambda row: (choice(numpy.arange(0, CV), p=[1/3]*CV), row)).persist(StorageLevel.DISK_ONLY)

    # for param in list(ParameterGrid(param_grid)):
    #     RMSE = []
    #     for val in range(0, CV):
    #         trainSet = trainCV.filter(lambda row: row[0]!=val).map(lambda row: row[1])
    #         valSet = trainCV.filter(lambda row: row[0]==val).map(lambda row: row[1])
    #         RMSE.extend([ModelBasedCF(trainSet, valSet, param['rank'], param['numIterations'], param['numlambda'])])
    #     print(param, ':', sum(RMSE)/CV)