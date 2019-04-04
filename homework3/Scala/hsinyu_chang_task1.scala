import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
//import util.control.Breaks._
//import scala.math.Ordering.Implicits._
import java.io._

object hsinyu_chang_task1 {

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

  def main(args: Array[String]) {

//    System.setProperty("java.util.Arrays.useLegacyMergeSort", "true")

    val Start = System.nanoTime

    val inputFilePath = args(0)
    val outputFilePath = args(1)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("hw3")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)

    val inputFile = sc.textFile(inputFilePath)
    val header = inputFile.first()

//    user_id,  business_id
    val inputRDD = inputFile.filter(row => row!=header).
      map(_.split(",")).map(row => (row(0), row(1))).persist(StorageLevel.DISK_ONLY)
//    all business
//    val businessList = inputRDD.map(_._2).distinct.collect.sorted

//    all users:
    val userIndex = inputRDD.map(_._1).distinct.collect.sorted.zipWithIndex.toMap
    val userBin = userIndex.size

//    indexing user:
//    (business_id, user_Index)
    val inputIndex = inputRDD.map(row => (row._2, userIndex(row._1))).persist(StorageLevel.DISK_ONLY)

//    grouping by business: every pair needs to group
    val inputGroup = inputIndex.groupByKey().mapValues(_.toSet).collect.toMap

//    number of minhash
    val minhash_num = 51
    val hash_para = HashParaGenerator(minhash_num, userBin)
    val bands = 18

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
    val PairsSim = candidate_pairs.map(pair => Jarccard(pair, inputGroup)).filter(sim => sim._2>=0.5).sortBy(pair => (pair._1._1, pair._1._2)).persist(StorageLevel.DISK_ONLY)

//    write txt file
    val headers = sc.parallelize(List("business_id_1, business_id_2, similarity"),1)
    val PairsSimOutput = PairsSim.map(t => s"${t._1._1},"+s"${t._1._2},"+f"${t._2}")
    val output = headers.union(PairsSimOutput).collect.mkString("\n")

    val writer = new PrintWriter(new File(outputFilePath))
    writer.write(output)
    writer.close()

    val end = System.nanoTime
    println("Duration: " + ((end-Start)/1000000000.0).toInt + " seconds" )
//
//
//    val Pairs = PairsSim.map(_._1)
//    val truth = sc.textFile("/Users/changhsinyu/Desktop/hsinyu_chang_hw3/pure_jaccard_similarity.csv")
//    val header1 = truth.first()
////    user_id,  business_id
//    val truth_pairs = truth.filter(_!=header1).map(_.split(",")).
//      map(row => (row(0), row(1))).persist(StorageLevel.DISK_ONLY)
//
//    val tp = Pairs.intersection(truth_pairs).count()
//    val fp = Pairs.subtract(truth_pairs).count()
//    val fn = truth_pairs.subtract(Pairs).count()
//    println("precision: " + tp.toFloat/(tp+fp))
//    println("recall: " + tp.toFloat/(tp+fn))
//
//    println("Duration: " + ((end-Start)/1000000000.0).toInt + " seconds" )

  }
}
