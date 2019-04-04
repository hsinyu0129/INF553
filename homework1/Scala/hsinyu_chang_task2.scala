import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import org.apache.spark.storage.StorageLevel
import org.json4s._
//import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.writePretty
import java.io._

object hsinyu_chang_task2 {

  implicit val formats = DefaultFormats

  def main(args: Array[String]) {

    val inputFile = args(0)
    val outputFile = args(1)
    val n_partition = args(2).toInt

    val conf = new SparkConf()

    conf.setMaster("local[*]")
    conf.setAppName("INF553")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)
    val reviews = sc.textFile(inputFile)

    val reviewsRDD = reviews.map(line => parse(line).extract[Map[String, String]]).
      map(review => (review("business_id"),1)).
      persist(StorageLevel.DISK_ONLY)

    val default_partition = reviewsRDD.getNumPartitions
    val default_n_items = reviewsRDD.mapPartitions(iter => Iterator(iter.size), true).collect()
    val defaultStart = System.nanoTime
    val defaultF = reviewsRDD.reduceByKey(_+_).sortBy(business => (-business._2, business._1)).take(10)
    val defaulttime = (System.nanoTime() - defaultStart)/1000000000.0


    val customizedRDD = reviewsRDD.partitionBy(new HashPartitioner(n_partition)).persist(StorageLevel.DISK_ONLY)
    val customized_n_items = customizedRDD.mapPartitions(iter => Iterator(iter.size), true).collect()

    val customizedStart = System.nanoTime
    val customizedF = customizedRDD.reduceByKey(_+_).sortBy(business => (-business._2, business._1)).take(10)
    val customizedtime = (System.nanoTime() - customizedStart)/1000000000.0



    val task2 = Map(
      "default" -> Map("n_partition" -> default_partition, "n_items" -> default_n_items, "exe_time" -> defaulttime ),
      "customized" -> Map("n_partition" -> n_partition, "n_items" -> customized_n_items, "exe_time" -> customizedtime ),
      "explanation"-> "If a proper number of partitions is chosen, using a customized partitioner, hash(business_id) % n_partition, will send items with same business_id into the same bucket, therefore we can reduce the shuffling costs when we need to operate reduceBy() on the 'business_id' key and save time.")

//
//    defaultF.foreach {
//      case (business, count) => { println(s"$business,"+f"$count%1.1f")
//      }
//    }
//
//    customizedF.foreach {
//      case (business, count) => { println(s"$business,"+f"$count%1.1f")
//      }
//    }

    val writer = new PrintWriter(new File(outputFile))
    writer.write(writePretty(task2))
    writer.close()

  }
}