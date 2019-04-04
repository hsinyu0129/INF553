import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.json4s._
//import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.writePretty
import java.io._

object hsinyu_chang_task3 {

  implicit val formats = DefaultFormats

  def main(args: Array[String]) {

    val inputFileA = args(0)
    val inputFileB = args(1)
    val outputFileA = args(2)
    val outputFileB = args(3)

    val conf = new SparkConf()

    conf.setMaster("local[*]")
    conf.setAppName("INF553")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)
    val reviews = sc.textFile(inputFileA)
    val businesss = sc.textFile(inputFileB)

    val reviewsRDD = reviews.map(line => parse(line).extract[Map[String, String]]).
      map(review => (review("business_id"), review("stars").toFloat))


    val businessRDD = businesss.map(line => parse(line).extract[Map[String, Any]]).
      map(business => (business("business_id").toString, business("city").toString))

    val a = businessRDD.join(reviewsRDD).map(b => b._2).
      aggregateByKey((0.0,0))(
        (U, star) => (U._1+star, U._2+1),
        (U, star) => (U._1+star._1, U._2+star._2)).
      map(city => (city._1, (city._2)._1/(city._2)._2)).
      sortBy(city => (-city._2, city._1)).persist(StorageLevel.DISK_ONLY)

    val header = sc.parallelize(List("city,stars"),1)
    val A = a.map(t => s"${t._1},"+f"${t._2}")
    header.union(A).coalesce(1).saveAsTextFile(outputFileA)


//    method1
    val m1Start = System.nanoTime
    val collectB = a.collect()
    collectB.take(10).foreach {
            case (city, star) => { println(s"$city,"+f"$star")
            }
          }
    val m1 = (System.nanoTime-m1Start)/1000000000.0

    //method1
    val m2Start = System.nanoTime
    a.take(10).foreach {
      case (city, star) => { println(s"$city,"+f"$star")
      }
    }
    val m2 = (System.nanoTime-m2Start)/1000000000.0


    val task3 = Map(
      "m1" -> m1,
      "m2" -> m2,
      "explanation"-> "In method1, all pairs are collected first, even though most of the pairs are not going to be used in the 'Printing' process. However, method2 just takes the first 10 pairs and stop. In sum, method2 is faster than method1."
    )

    val writer = new PrintWriter(new File(outputFileB))
    writer.write(writePretty(task3))
    writer.close()

  }
}
