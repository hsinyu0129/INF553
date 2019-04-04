import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.graphframes._
import java.io._

object hsinyu_chang_task1 {

  def main(args: Array[String]) {

    val Start = System.nanoTime

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("hw")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "4")


    val spark = SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val threshold = args(0).toInt
    val inputFilePath = args(1)
    val outputFilePath = args(2)

    val inputFile = sc.textFile(inputFilePath)
    val header = inputFile.first()
    val inputRDD = inputFile.filter(row => row!=header).
      map(_.split(",")).map(row => (row(0), row(1))).persist(StorageLevel.DISK_ONLY)

//    build undirected graph
//    (user_id1, user_id2)
    val edgesRDD = inputRDD.map(row => (row._2, row._1)).groupByKey.map(_._2.toArray.sorted).
      flatMap(_.combinations(2).map{ case Array(p1, p2) => ((p1, p2), 1)}).reduceByKey(_+_).
      filter(_._2 >= threshold).flatMap(edge => Array(edge._1, (edge._1._2, edge._1._1)))

//    (user_id, id)
    val verticesIndex = edgesRDD.map(edge => edge._1).distinct.zipWithIndex.map(user => (user._1, user._2.toString)).persist(StorageLevel.DISK_ONLY)
//    user_id: id
    val verticesDict = verticesIndex.collect.toMap

//    id, user_id
    val vertices = verticesIndex.map(user => (user._2, user._1)).toDF("id", "user_id").persist(StorageLevel.DISK_ONLY)
//    (id1, id2)
    val edges = edgesRDD.map(edges=> (verticesDict(edges._1), verticesDict(edges._2))).toDF("src", "dst").persist(StorageLevel.DISK_ONLY)
    val graph = GraphFrame(vertices, edges)

    val communities = graph.labelPropagation.maxIter(5).run()

//    same label > same community size > order by community size
    val communitiesRDD = communities.rdd.map(row => (row(2).toString, row(1).toString)).groupByKey.
      map(com => (com._2.size, com._2.toArray.sorted.mkString("'", "', '", "'"))).groupByKey.sortBy(_._1).
      map(degree => degree._2.toArray.sorted.mkString("\n"))

    val output = communitiesRDD.collect.mkString("\n")
    val writer = new PrintWriter(new File(outputFilePath))
    writer.write(output)
    writer.close()

    val end = System.nanoTime
    println("Duration: " + ((end-Start)/1000000000.0).toInt + " seconds" )
  }
}


//    val vertices = spark.createDataFrame(List(
//      ("a", "Alice"),
//      ("b", "Bob"),
//      ("c", "Charlie"),
//      ("d", "David"),
//      ("e", "Esther"),
//      ("f", "Fanny"),
//      ("g", "Gabby")
//    )).toDF("id", "name")
//    // Edge DataFrame
//    val edges = spark.createDataFrame(List(
//      ("a", "b"),
//      ("b", "c"),
//      ("c", "b"),
//      ("f", "c"),
//      ("e", "f"),
//      ("e", "d"),
//      ("d", "a"),
//      ("a", "e")
//    )).toDF("src", "dst")

