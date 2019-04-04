import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{HashMap, ListBuffer}
//import util.control.Breaks._
import scala.math.Ordering.Implicits._
import java.io._

object hsinyu_chang_task1 {

  def SortLists(l1: List[String], l2: List[String]) : Boolean = { l1<=l2 }

  def Size3Candidate(preFreq: List[Set[String]], k: Int): List[Set[String]] = {
    val candidates = new ListBuffer[Set[String]]
    var temp = new ListBuffer[List[String]]
    preFreq.foreach(temp+=_.toList.sorted)
    val CutpreFreq = temp.toList.groupBy(_.dropRight(1))

    for (( _, bro) <- CutpreFreq){
      if(bro.size >=2){
        for{
          (b1, ib1) <- bro.zipWithIndex
          (b2, ib2) <- bro.zipWithIndex
          if ib1<ib2
        } candidates += b1.toSet.union(b2.toSet)
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

  def countOnWhole(basket : Set[String], candidates: List[Set[String]]): List[(Set[String], Int)]={
    for(candy <- candidates if candy.subsetOf(basket)) yield (candy, 1)
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

    val Case = args(0)
    val support = args(1).toInt
    val inputFilePath = args(2)
    val outputFilePath = args(3)

//    val Case = "1"
//    val support = 4
//    val inputFilePath = "/Users/changhsinyu/Desktop/hsinyu_chang_hw2/small2.csv"
//    val outputFilePath = "/Users/changhsinyu/Desktop/hsinyu_chang_hw2/output/scalatask1case1.txt"


    val conf = new SparkConf()

    conf.setMaster("local[*]")
    conf.setAppName("INF553")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(inputFilePath)
    val header = inputFile.first()

    val inputRDD = if (Case == "1") {
      inputFile.filter(line => line != header).
        map(_.split(",")).map(a => (a(0), a(1))).
        groupByKey.map(t => (t._1, t._2.toSet)).map(t => (1, t._2)).persist(StorageLevel.DISK_ONLY)
    }else {
      inputFile.filter(line => line != header).
        map(_.split(",")).map(a => (a(1), a(0))).
        groupByKey.map(t => (t._1, t._2.toSet)).map(t => (1, t._2)).persist(StorageLevel.DISK_ONLY)
    }

    //    count distinct items
    val distinctItems = inputRDD.flatMapValues(value => value).
      map(t => (t._2, t._1)).groupByKey().
      map(t => t._1).map(Set(_)).collect().toList

    //    whole baskets number
    val whole_number = inputRDD.count()

    val baskets = inputRDD.map(_._2).persist(StorageLevel.DISK_ONLY)

    //    first phase > find candidate in subsets
    val FreqinSample = baskets.mapPartitions(freqinSubset(_, distinctItems, support, whole_number)).flatMap(value => List(value)).distinct().collect().toList

    //    second phase > find frequents in whole dataset
    val frequentI = baskets.flatMap(countOnWhole(_, FreqinSample)).flatMap(value => List(value)).reduceByKey(_+_).filter(_._2 >= support).map(_._1).collect.toList

    //    output
    val FreqinSampleStr = "Candidates:\n" + strItemSets(FreqinSample)
    val frequentIStr = "Frequent Itemsets:\n" + strItemSets(frequentI)

    val writer = new PrintWriter(new File(outputFilePath))
    writer.write(FreqinSampleStr)
    writer.write("\n\n"+ frequentIStr)
    writer.close()

    println("Duration: " + ((System.nanoTime-Start)/1000000000.0).toInt + " seconds" )

  }

}
