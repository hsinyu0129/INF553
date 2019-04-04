import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{HashMap, ListBuffer, ArrayBuffer}
import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}
import breeze.numerics.{abs, pow}
import scala.math.min
import java.io._


object hsinyu_chang_task2 {
  //  for LSH //////////////
  def isPrime(num: Int): Boolean = (2 to math.sqrt(num).toInt) forall ( p => num % p != 0)

  def findPrime(number: Int): Int = {
    val temp = (number+1 to number+10000).iterator.takeWhile(!isPrime(_)).toList
    if(temp.size == 0) return number+1
    return temp.last+1
  }

  def HashParaGenerator(number: Int, bin: Int): Map[Char, Seq[Int]] = {
    val random = scala.util.Random
    val a = for (i <- 1 to number) yield random.nextInt(number*100)
    val b = for (i <- 1 to number) yield random.nextInt(number*100)
    var p = new ListBuffer[Int]
    (for (i <- 1 to number) yield random.nextInt(number*100)).foreach(p += findPrime(_))

    return Map('a' -> a, 'b' -> b, 'p' -> p, 'm' -> Seq(findPrime(bin)))
  }

  def hashValue(row: (String, Int), hpara: Map[Char, Seq[Int]]): Seq[((String, Int), Int)] = {
    val a = hpara('a')
    val b = hpara('b')
    val p = hpara('p')
    val m = hpara('m')(0)
    for( h <- 0 until a.size) yield ((row._1, h),(((row._2*a(h)+b(h))%p(h))%m))
  }

  def Jarccard(pair: (String, String), inputGroup: Map[String, Set[Int]]): ((String, String), Float) = {
    val set1 = inputGroup(pair._1)
    val set2 = inputGroup(pair._2)
    return (pair, (set1&set2).size.toFloat/(set1|set2).size)
  }
  //  for LSH //////////////

  def rateInterval(rate: Double): Double={
    if(rate<0) return 0.toDouble
    if(rate>5) return 5.toDouble
    return rate
  }

  def CartesianProduct(array: List[(String, Double)], pairs: Array[(String, String)]): Array[((String, String), ArrayBuffer[(Double, Double)])]={
    val dic = array.toMap
    for(pair <- pairs; if (dic.contains(pair._1) & dic.contains(pair._2))) yield (pair, ArrayBuffer((dic(pair._1), dic(pair._2))))
  }

  def Valid2Pair(iterator: Iterator[List[(String, Double)]], support: Int, numofwhole: Long): Iterator[((String, String), Int)] = {
    var basketsLen = 0
    val tempcount = HashMap.empty[(String, String), Int]

    for (basket <- iterator) {
      basketsLen += 1
      val lst = basket.sortBy(_._1)
      for (pairs <- lst.combinations(2)) {
        if (tempcount.contains((pairs(0)._1, pairs(1)._1))) {
          val temp = tempcount((pairs(0)._1, pairs(1)._1))
          tempcount((pairs(0)._1, pairs(1)._1)) = temp + 1
        } else {
          tempcount((pairs(0)._1, pairs(1)._1)) = 1
        }
      }
    }

    val validpair = new ArrayBuffer[((String, String), Int)]
    val sub_s = (support * basketsLen / numofwhole).toInt

    for((pair, count) <- tempcount) {
      if(count >= sub_s) validpair += ((pair, count))
    }
    validpair.toIterator
  }

  def Pearson(arrays: ArrayBuffer[(Double, Double)]): Double = {
    val u1 = arrays.toList.map(_._1)
    val u2 = arrays.toList.map(_._2)
    val (avg1, avg2) = (u1.reduce(_+_)/u1.length, u2.reduce(_+_)/u2.length)
    val up = arrays.toList.map(rates => (rates._1-avg1)*(rates._2-avg2)).reduce(_+_)
    val left = u1.map(r1 => pow(r1-avg1,2)).reduce(_+_)
    val right = u2.map(r2 => pow(r2-avg2,2)).reduce(_+_)

    if(u1.length == 1 | left == 0 | right == 0) return 0.toDouble
    return up/pow(left, 0.5)/(pow(right, 0.5))

  }

  def SplitSimilarity(sim: ((String, String), Double)): Iterator[(String, (String, Double))] = Iterator((sim._1._1, (sim._1._2, sim._2)), (sim._1._2, (sim._1._1, sim._2)))

  def topSimilar(num: Int, sim: Iterable[(String, Double)]): List[(String, Double)] = sim.toList.sortBy(-_._2).slice(0, min(sim.toList.length, num))

  def predictionU(testpair: (String, String), avgsDict: Map[String,Double], UserSimDict: Map[String, Map[String,Double]], UserDict: Map[String, Map[String,Double]], avgall: Double): Double = {
    if(!(avgsDict isDefinedAt testpair._1)) return avgall
    if(!(UserSimDict isDefinedAt testpair._1)) return avgsDict(testpair._1)
    if(!(UserSimDict(testpair._1).nonEmpty)) return avgsDict(testpair._1)

    val up = new ListBuffer[Double]
    val down = new ListBuffer[Double]

    UserSimDict(testpair._1) foreach ( (user) =>
      if (UserDict(user._1).contains(testpair._2)){
        up += (UserDict(user._1)(testpair._2) - avgsDict(user._1))*UserSimDict(testpair._1)(user._1)
        down += abs(UserSimDict(testpair._1)(user._1))
      }
      )

    if(down.nonEmpty) return rateInterval(avgsDict(testpair._1)+up.sum/down.sum)
    else avgsDict(testpair._1)
  }

  def predictionI(testpair: (String, String), avgsDict: Map[String,Double], ItemSimDict: Map[String, Map[String,Double]], UserDict: Map[String, Map[String,Double]], avgall: Double): Double = {
    if(!(avgsDict isDefinedAt testpair._1)) return avgall
    if(!(ItemSimDict isDefinedAt testpair._2)) return avgsDict(testpair._1)
    if(!(ItemSimDict(testpair._2).nonEmpty)) return avgsDict(testpair._1)

    val up = new ListBuffer[Double]
    val down = new ListBuffer[Double]

    ItemSimDict(testpair._2) foreach ((item) =>
      if (UserDict(testpair._1).contains(item._1)){
        up += UserDict(testpair._1)(item._1)*ItemSimDict(testpair._2)(item._1)
        down += abs(ItemSimDict(testpair._2)(item._1))
      }
      )

    if(down.nonEmpty) return rateInterval(up.sum/down.sum)
    else avgsDict(testpair._1)
  }

  def ModelBasedCF(trainRDD: RDD[(String, String, Double)], testRDD: RDD[(String, String, Double)]): RDD[((String, String), Double)] = {
    //    Indexing
    val userIndex = trainRDD.map(_._1).union(testRDD.map(_._1)).distinct.zipWithUniqueId.collect.toMap
    val indexUser = userIndex.map(_.swap)

    val businessIndex = trainRDD.map(_._2).union(testRDD.map(_._2)).distinct().zipWithUniqueId.collect.toMap
    val indexBusiness = businessIndex.map(_.swap)

    val avgsDict = trainRDD.map(row => (row._1, row._3)).
      aggregateByKey((0.0,0))(
        (U, rate) => (U._1+rate, U._2+1),
        (U, rate) => (U._1+rate._1, U._2+rate._2)).
      map(rates => (userIndex(rates._1), (rates._2)._1/(rates._2)._2)).collect.toMap

    val avgall = trainRDD.map(_._3).mean

    //    Indexing
    val trainRating = trainRDD.map(row => Rating(userIndex(row._1).toInt, businessIndex(row._2).toInt, row._3))
    val testPair = testRDD.map(row => (userIndex(row._1).toInt, businessIndex(row._2).toInt))
    val (numIterations, numlambda, rank) = (10, 0.05, 3)

    //    modeling
    val model = ALS.train(trainRating, rank, numIterations, numlambda)

    //    prediction
    val predictSub = model.predict(testPair).map {
      case Rating(user, business, rate) => ((user.toLong, business.toLong), rateInterval(rate))}
    val predictAvg = testPair.map(pair => ((pair._1.toLong, pair._2.toLong), null)).subtractByKey(predictSub).
      map(pair=> if (avgsDict.contains(pair._1._1)) ((indexUser(pair._1._1), indexBusiness(pair._1._2)), rateInterval(avgsDict(pair._1._1))) else ((indexUser(pair._1._1), indexBusiness(pair._1._2)), avgall))
    val predictRate = predictSub.map(r => ((indexUser(r._1._1), indexBusiness(r._1._2)), rateInterval(r._2))).union(predictAvg)

    return predictRate
  }

  def UserBasedCF(trainRDD: RDD[(String, String, Double)], testRDD: RDD[(String, String, Double)]): RDD[((String, String), Double)] = {

    val trainRDD_user = trainRDD.map(row => (row._1, (row._2, row._3))).groupByKey.persist(StorageLevel.DISK_ONLY)
    val UserDict = trainRDD_user.mapValues(_.toMap).collect.toMap

    //    user, avg
    val avgsDict = trainRDD.map(row => (row._1, row._3)).
      aggregateByKey((0.0,0))(
          (U, rate) => (U._1+rate, U._2+1),
          (U, rate) => (U._1+rate._1, U._2+rate._2)).
          map(rates => (rates._1, (rates._2)._1/(rates._2)._2)).collect.toMap

    val avgall = trainRDD.map(_._3).mean

    //    find co-rated user couple
    val trainRDD_item = trainRDD.map(row => (row._2, (row._1, row._3))).groupByKey.map(_._2.toList).filter(_.length>1).persist(StorageLevel.DISK_ONLY)
    val numofitems = trainRDD_item.count
    val support = 12

    //    get Valid User Pairs ( with co-rated >= support)
    val UserPair = trainRDD_item.mapPartitions(Valid2Pair(_, support, numofitems)).reduceByKey(_+_).filter(_._2>=support).
      map(_._1).collect

    val UserSim = trainRDD_item.flatMap(users => CartesianProduct(users, UserPair)).
      reduceByKey(_++_).mapValues(Pearson(_)).filter(sim => abs(sim._2)>0).flatMap(sim => SplitSimilarity(sim)).
      groupByKey.map(sim => (sim._1, topSimilar(3, sim._2).toMap)).collect

    val UserSimDict = UserSim.toMap

    //    prediction
    val testPair = testRDD.map(row => (row._1, row._2))
    val predictRate = testPair.map(pair => (pair, predictionU(pair, avgsDict, UserSimDict, UserDict, avgall))).persist(StorageLevel.DISK_ONLY)
    return predictRate
  }

  def ItemBasedCF(trainRDD: RDD[(String, String, Double)], testRDD: RDD[(String, String, Double)]): RDD[((String, String), Double)] = {

    val trainRDD_user = trainRDD.map(row => (row._1, (row._2, row._3))).groupByKey.persist(StorageLevel.DISK_ONLY)
    val UserDict = trainRDD_user.mapValues(_.toMap).collect.toMap

    //    user, avg
    val avgsDict = trainRDD.map(row => (row._1, row._3)).
      aggregateByKey((0.0,0))(
        (U, rate) => (U._1+rate, U._2+1),
        (U, rate) => (U._1+rate._1, U._2+rate._2)).
      map(rates => (rates._1, (rates._2)._1/(rates._2)._2)).collect.toMap

    val avgall = trainRDD.map(_._3).mean

    //    find co-rated user couple
    val trainRDD_userG = trainRDD_user.map(_._2.toList).filter(_.length>1).persist(StorageLevel.DISK_ONLY)
    val numofusers = trainRDD_userG.count
    val support = 8

    //    get Valid User Pairs ( with co-rated >= support)
    val ItemPair = trainRDD_userG.mapPartitions(Valid2Pair(_, support, numofusers)).reduceByKey(_+_).filter(_._2>=support).
      map(_._1).collect
    //    val ItemPair = trainRDD_userG.mapPartitions(Valid2Pair(_, support, numofusers)).map(_._1).distinct.collect

    //    get Valid User Pairs ( with co-rated > support)
    val ItemSim = trainRDD_userG.flatMap(items => CartesianProduct(items, ItemPair)).
      reduceByKey(_++_).mapValues(Pearson(_)).filter(sim => abs(sim._2)>0).flatMap(sim => SplitSimilarity(sim)).
      groupByKey.map(sim => (sim._1, topSimilar(1, sim._2).toMap)).collect
    //
    val ItemSimDict = ItemSim.toMap

    //    prediction
    val testPair = testRDD.map(row => (row._1, row._2))
    val predictRate = testPair.map(pair => (pair, predictionI(pair, avgsDict, ItemSimDict, UserDict, avgall))).persist(StorageLevel.DISK_ONLY)

    return predictRate
  }

  def ItemBasedCFwLSH(trainRDD: RDD[(String, String, Double)], testRDD: RDD[(String, String, Double)]): RDD[((String, String), Double)] = {

    val trainRDD_user = trainRDD.map(row => (row._1, (row._2, row._3))).groupByKey.persist(StorageLevel.DISK_ONLY)
    val UserDict = trainRDD_user.mapValues(_.toMap).collect.toMap

    //    user, avg
    val avgsDict = trainRDD.map(row => (row._1, row._3)).
      aggregateByKey((0.0,0))(
        (U, rate) => (U._1+rate, U._2+1),
        (U, rate) => (U._1+rate._1, U._2+rate._2)).
      map(rates => (rates._1, (rates._2)._1/(rates._2)._2)).collect.toMap

    val avgall = trainRDD.map(_._3).mean

    // LSHHHHHHH
    //    all users:
    val userIndex = trainRDD.map(_._1).distinct.collect.sorted.zipWithIndex.toMap
    val userBin = userIndex.size

    //    indexing user:
    //    (business_id, user_Index)
    val inputIndex = trainRDD.map(row => (row._2, userIndex(row._1))).persist(StorageLevel.DISK_ONLY)

    //    grouping by business: every pair needs to group
    val inputGroup = inputIndex.groupByKey().mapValues(_.toSet).collect.toMap

    //    number of minhash
    val minhash_num = 36
    val hash_para = HashParaGenerator(minhash_num, userBin)
    val bands = 13

    //    minhash
    val signs = inputIndex.flatMap(row => hashValue(row, hash_para)).reduceByKey((x, y) => if(x >= y) y else x).persist(StorageLevel.DISK_ONLY)
    //    output > ((business_id, hashindex), minhashvalue)

    val signs_bands = signs.map(row => ((row._1._2%bands, row._1._1), (row._1._2, row._2))).groupByKey().
      map(row => (row._2.toSet, row._1._2)).groupByKey().map(business => business._2.toList.sorted).
      filter(bucket => bucket.size > 1).persist(StorageLevel.DISK_ONLY)
    //    output > (bandIndex, business_id), (hashIndex, hashValue)
    //    output > (Set(hashIndex, hashValue)), business_id (filter out bucket have more than one business_id to make similar pairs)
    //    output > business_ids

    val candidate_pairs = signs_bands.flatMap(_.combinations(2).map{ case Seq(p1, p2) => (p1, p2)}).
      distinct().sortBy(pair => (pair._1, pair._2)).persist(StorageLevel.DISK_ONLY)

    //    Jaccard similarity for every candidate pairs from LSH
    val ItemPair = candidate_pairs.map(pair => Jarccard(pair, inputGroup)).filter(sim => sim._2>=0.2).map(_._1).collect

    val trainRDD_userG = trainRDD_user.map(_._2.toList).filter(_.length>1).persist(StorageLevel.DISK_ONLY)

    //    get Valid User Pairs ( with co-rated > support)
    val ItemSim = trainRDD_userG.flatMap(items => CartesianProduct(items, ItemPair)).
      reduceByKey(_++_).mapValues(Pearson(_)).filter(sim => abs(sim._2)>0).flatMap(sim => SplitSimilarity(sim)).
      groupByKey.map(sim => (sim._1, topSimilar(1, sim._2).toMap)).collect
    //
    val ItemSimDict = ItemSim.toMap

    //    prediction
    val testPair = testRDD.map(row => (row._1, row._2))
    val predictRate = testPair.map(pair => (pair, predictionI(pair, avgsDict, ItemSimDict, UserDict, avgall))).persist(StorageLevel.DISK_ONLY)

    return predictRate

  }


  def main(args: Array[String]){

    val Start = System.nanoTime

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("hw3")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.heartbeatInterval", "60s")
    val sc = new SparkContext(conf)


    val trainFilePath = args(0)
    val testFilePath = args(1)
    val Case = args(2)
    val outputFilePath = args(3)

    //    Trainfile
    val trainFile = sc.textFile(trainFilePath,3)
    val train_header = trainFile.first()
    //    user_id,  business_id, ratings
    val trainRDD = trainFile.filter(row => row!=train_header).
      map(_.split(",")).map(row => (row(0), row(1), row(2).toDouble)).persist(StorageLevel.DISK_ONLY)

    //    Testfile
    val testFile = sc.textFile(testFilePath)
    val test_header = testFile.first()
    //    user_id,  business_id, ratings
    val testRDD = testFile.filter(row => row!=test_header).
      map(_.split(",")).map(row => (row(0), row(1), row(2).toDouble)).persist(StorageLevel.DISK_ONLY)


    val predictRate = if (Case == "1") {
           ModelBasedCF(trainRDD, testRDD)
    }else if (Case == "2") {
          UserBasedCF(trainRDD, testRDD)
    }else if (Case == "3") {
          ItemBasedCF(trainRDD, testRDD)
    }else {
         ItemBasedCFwLSH(trainRDD, testRDD)
    }

    //    write txt file
    val headers = sc.parallelize(List("user_id, business_id, prediction"),1)
    val PairsSimOutput = predictRate.map(t => s"${t._1._1},"+s"${t._1._2},"+f"${t._2}")
    val output = headers.union(PairsSimOutput).collect.mkString("\n")

    val writer = new PrintWriter(new File(outputFilePath))
    writer.write(output)
    writer.close()

    val end = System.nanoTime
    println("Duration: " + ((end-Start)/1000000000.0).toInt + " seconds" )

//    //    Evaluation
//    val evalu = testRDD.map(row => ((row._1, row._2), row._3)).join(predictRate).persist(StorageLevel.DISK_ONLY)
//    val distribution = evalu.map(r => abs(r._2._1 - r._2._2.toDouble)).persist(StorageLevel.DISK_ONLY)
//    val MSE = evalu.map(r => pow(r._2._1 - r._2._2, 2)).mean()
//
//    val diff1 = distribution.filter(dif => dif >= 0 && dif < 1).count()
//    val diff2 = distribution.filter(dif => dif >= 1 && dif < 2).count()
//    val diff3 = distribution.filter(dif => dif >= 2 && dif < 3).count()
//    val diff4 = distribution.filter(dif => dif >= 3 && dif < 4).count()
//    val diff5 = distribution.filter(dif => dif >= 4).count()
//
//
//    println(s">=0 and <1: $diff1")
//    println(s">=1 and <2: $diff2")
//    println(s">=2 and <3: $diff3")
//    println(s">=3 and <4: $diff4")
//    println(s">=4: $diff5")
//    println("RMSE: "+ pow(MSE,0.5).toString)
//    println("Duration: " + ((end-Start)/1000000000.0).toInt + " seconds" )
//
//    //    case1 >  33 s RMSE = 1.143
//    //    case2 >  86 s RMSE = 1.109
//    //    case3 > 120 s RMSE = 1.075
//    //    case4 >  73 s RMSE = 1.075

  }
}
