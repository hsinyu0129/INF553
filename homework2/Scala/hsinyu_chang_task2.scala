import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{HashMap, ListBuffer}
//import util.control.Breaks._
import scala.math.Ordering.Implicits._
import java.io._

object hsinyu_chang_task2 {

  def SortLists(l1: List[String], l2: List[String]): Boolean = { l1 <= l2 }

  def Size3Candidate(preFreq: List[Set[String]], k: Int): List[Set[String]] = {
    val candidates = new ListBuffer[Set[String]]
    for ((f1, index) <- preFreq.slice(0, preFreq.length).zipWithIndex) {
      for (f2 <- preFreq.slice(index + 1, preFreq.length)) {
        var comb = f1.union(f2)
        if (comb.size == k) {
          candidates += comb
        }
      }
    }
    candidates.toList.distinct
  }

  def freqinSubset(iterator: Iterator[Set[String]], distinctItems: List[Set[String]], support: Int, numofwhole: Long): Iterator[Set[String]] = {
    val baskets = iterator.toList
    val sub_s = (support * baskets.length / numofwhole).toInt
    val freqSub = HashMap.empty[Int, List[Set[String]]]
    var k = 0
    var candies = distinctItems
    var temp = new ListBuffer[String]
    var flatSingle = Set.empty[String]

    var allsubFreq = new ListBuffer[Set[String]]

    while (candies.nonEmpty) {
      k += 1

      val tempcount = HashMap.empty[Set[String], Int]

      for (basket <- baskets) {

        if (k == 1) {
          for (single <- basket.sliding(1)) {
            if (tempcount.contains(single)) {
              val temp = tempcount(single)
              tempcount(single) = temp + 1
            } else {
              tempcount(single) = 1
            }
          }
        }

        if (k == 2) {
          val thinbasket = basket.intersect(flatSingle)
          for (pair <- thinbasket.subsets(2)) {
            if (tempcount.contains(pair)) {
              val temp = tempcount(pair)
              tempcount(pair) = temp + 1
            } else {
              tempcount(pair) = 1
            }
          }
        }

        if (k > 2) {
          val thinbasket = basket.intersect(flatSingle)
          if (thinbasket.size >= k) {
            for (candy <- candies) {
              if (candy.subsetOf(thinbasket)) {
                if (tempcount.contains(candy)) {
                  val temp = tempcount(candy)
                  tempcount(candy) = temp + 1
                } else {
                  tempcount(candy) = 1
                }
              }
            }
          }
        }
      }

      val tempFreq = new ListBuffer[Set[String]]

      for ((key, v) <- tempcount) {
        if (v >= sub_s) tempFreq += key
      }

      allsubFreq ++= tempFreq.toList

      if (k == 1) {
        tempFreq.toList.foreach {
          temp += _.last
        }
        candies = temp.toSet.subsets(2).toList
        flatSingle ++= temp.toSet

      }

      if (k > 1) {
        candies = Size3Candidate(tempFreq.toList, k + 1)
      }
    }

    allsubFreq.iterator
  }

  def countOnWhole(basket: Set[String], candidates: List[Set[String]]): List[(Set[String], Int)] = {
    for (candy <- candidates if candy.subsetOf(basket)) yield (candy, 1)
  }

  def strItemSets(ItemSets: List[Set[String]]): String = {
    val CutSets = ItemSets.groupBy(_.size)
    var output = ""
    for (p <- 1 until CutSets.size + 1) {
       var cuts = new ListBuffer[String]

      CutSets(p).foreach {
        cuts += _.toList.sorted.mkString("('", "', '", "')")
      }
      output += cuts.toList.sorted.mkString(",")
      output += "\n\n"

    }
    output.dropRight(2)
  }

  def main(args: Array[String]) {

    System.setProperty("java.util.Arrays.useLegacyMergeSort", "true")

    val Start = System.nanoTime

    val threshold = args(0).toInt
    val support = args(1).toInt
    val inputFilePath = args(2)
    val outputFilePath = args(3)

//    val threshold = "70".toInt
//    val support = "50".toInt
//    val inputFilePath = "/Users/changhsinyu/Desktop/hsinyu_chang_hw2/user_business.csv"
//    val outputFilePath = "/Users/changhsinyu/Desktop/hsinyu_chang_hw2/output/scalatask2.txt"

    val conf = new SparkConf()

    conf.setMaster("local[*]")
    conf.setAppName("INF553")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
//    conf.set("spark.network.timeout", "200s")
    conf.set("spark.executor.heartbeatInterval", "60s")

    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(inputFilePath)
    val header = inputFile.first()


    val inputRDD = inputFile.filter(line => line != header).
      map(_.split(",")).map(a => (a(0), a(1))).
      groupByKey.map(t => (t._1, t._2.toSet))

    val inputRDDfilter = inputRDD.filter(t => t._2.size > threshold).
      map(t => (1, t._2)).persist(StorageLevel.DISK_ONLY)


    //    count distinct items
    val distinctItems = inputRDDfilter.flatMapValues(value => value).
      map(t => (t._2, t._1)).groupByKey().
      map(t => t._1).map(Set(_)).collect().toList


    //    whole baskets number
    val whole_number = inputRDDfilter.count()

    val baskets = inputRDDfilter.map(_._2).persist(StorageLevel.DISK_ONLY)


    //    first phase > find candidate in subsets
    val FreqinSample = baskets.mapPartitions(freqinSubset(_, distinctItems, support, whole_number)).flatMap(value => List(value)).distinct().collect().toList

    //    second phase > find candidate in subsets
    val frequentI = baskets.flatMap(countOnWhole(_, FreqinSample)).flatMap(value => List(value)).reduceByKey(_ + _).filter(_._2 >= support).map(_._1).collect.toList

    //    output
    val FreqinSampleStr = "Candidates:\n" + strItemSets(FreqinSample)
    val frequentIStr = "Frequent Itemsets:\n" + strItemSets(frequentI)

    val writer = new PrintWriter(new File(outputFilePath))
    writer.write(FreqinSampleStr)
    writer.write("\n\n" + frequentIStr)
    writer.close()

    println("Duration: " + ((System.nanoTime-Start)/1000000000.0).toInt + " seconds" )

  }

}
