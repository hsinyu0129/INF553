import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.json4s._
//import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.writePretty
import java.io._

object hsinyu_chang_task1 {

    implicit val formats = DefaultFormats

    def main(args: Array[String]) {

        val inputFile = args(0)
        val outputFile = args(1)

        val conf = new SparkConf()

        conf.setMaster("local[*]")
        conf.setAppName("INF553")
        conf.set("spark.executor.memory", "4g")
        conf.set("spark.driver.memory", "4g")

        val sc = new SparkContext(conf)
        val reviews = sc.textFile(inputFile)

        val reviewsRDD = reviews.map(line => parse(line).extract[Map[String, String]]).
          map(review => (review("date"), review("user_id"), review("business_id"))).
          persist(StorageLevel.DISK_ONLY)

        val a = reviewsRDD.count()
        val b = reviewsRDD.filter(review => review._1.slice(0, 4) == "2018").count()


        val d = reviewsRDD.map(review => (review._2, 1)).reduceByKey(_ + _).
          sortBy(user => (-user._2, user._1)).
          persist(StorageLevel.DISK_ONLY)

        val c = d.count()
        val outputd =  d.map(user => List(user._1,user._2)).take(10)


        val f = reviewsRDD.map(review => (review._3, 1)).reduceByKey(_ + _).
          sortBy(business => (-business._2, business._1)).
          persist(StorageLevel.DISK_ONLY)

        val e = f.count()
        val outputf =  f.map(business => List(business._1,business._2)).take(10)


        val json = Map(
            "n_review" -> a,
            "n_review_2018" -> b,
            "n_user" -> c,
            "top10_user" -> outputd ,
            "n_business" -> e,
            "top10_business" -> outputf)


        val writer = new PrintWriter(new File(outputFile))
        writer.write(writePretty(json))
        writer.close()
    }
}
