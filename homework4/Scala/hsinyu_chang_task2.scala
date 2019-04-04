import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{Set, ArrayBuffer, HashMap}
import java.io._

object hsinyu_chang_task2 {

  class Graph(val originalG: Map[String, Set[String]]) {

    var adjacentDict = new HashMap[String, Set[String]] ++= originalG
    var Community = new ArrayBuffer[collection.mutable.Set[String]]
    var maxModularity = (-2147483648).toDouble

    def getVertices(): Array[String] = {
      return originalG.keySet.toArray
    }

    def getDegree(vertex: String): Int = {
      return originalG(vertex).size
    }

    def getallDegree(): Map[String, Int] = {
      return originalG.transform((_, value) => value.size)
    }

    def get2Edges(): Int = {
      return getallDegree().values.sum
    }

    def deleteEdge(v1: String, v2: String): Unit = {
      //      adjacentDict(v1).remove(v2)
      //      adjacentDict(v2).remove(v1)
      var tmp1 =  Set[String]()
      tmp1 ++= adjacentDict(v1).diff(Set(v2))
      adjacentDict(v1) = tmp1
      var tmp2 =  Set[String]()
      tmp2 ++= adjacentDict(v2).diff(Set(v1))
      adjacentDict(v2) = tmp2

      if (adjacentDict(v1).isEmpty) adjacentDict -= v1
      if (adjacentDict(v2).isEmpty) adjacentDict -= v2
    }

    def BFS(root: String): HashMap[String, (Int, ArrayBuffer[String])] = {

      val visited = new HashMap[String, (Int, ArrayBuffer[String])]
      val queue = new ArrayBuffer[String]

      visited(root) = (0, ArrayBuffer())
      queue += root
      if(!adjacentDict.contains(root)) return visited

      while (queue.nonEmpty) {
        val current = queue.remove(0)
        for (adj <- adjacentDict(current)) {
          if (!visited.contains(adj)) {
            queue += adj
            visited(adj) = (visited(current)._1 + 1, ArrayBuffer(current))
          } else if (visited(current)._1 + 1 == visited(adj)._1) visited(adj)._2 += current
        }
      }
      return visited
    }

    def Betweeness(): HashMap[(String, String), Double] = {

      val between = new HashMap[(String, String), Double]
      for (vertex <- getVertices()) {

        val parentnode = new ArrayBuffer[String]
        val level = new HashMap[Int, ArrayBuffer[(String, ArrayBuffer[String])]]
        val shortest_path = new HashMap[String, Int]

        var maxdegree = -1
        val bfstree = BFS(vertex)

        for ((v, l) <- bfstree) {
          if (!level.contains(l._1)) {
            level(l._1) = ArrayBuffer((v, l._2))
            if (l._1 > maxdegree) maxdegree = l._1
          } else level(l._1) += ((v, l._2))
          parentnode ++= l._2
        }

        for(l <- 0 to maxdegree){
          for ((child, parents) <- level(l)){
            if(parents.isEmpty) shortest_path(child) = 1
            else{
              var tmp = 0
              parents.foreach(tmp+=shortest_path(_))
              shortest_path(child) = tmp
            }
          }
        }

        val parentSet = parentnode.toSet
        val nodeweight = new HashMap[String, Double]

        for (degree <- (0 to maxdegree).reverse) {
          val nodes = level(degree)

          for ((child, parents) <- nodes) {
            //        not leaf
            if (parentSet contains child) nodeweight(child) += 1
            else nodeweight(child) = 1

            var allparents = 0
            parents.foreach(allparents+=shortest_path(_))

            for (parent <- parents) {
              if (nodeweight.contains(parent)) {
                val tmp = nodeweight(parent)
                nodeweight(parent) = tmp + nodeweight(child) * shortest_path(parent) / allparents.toDouble
              } else nodeweight(parent) = nodeweight(child) * shortest_path(parent) / allparents.toDouble

              val (v1, v2) = if (parent < child) {
                (parent, child)
              }
              else {
                (child, parent)
              }

              if (between.contains((v1, v2))) {
                val tmp = between((v1, v2))
                between((v1, v2)) = tmp + nodeweight(child) * shortest_path(parent) / allparents.toDouble
              } else {
                between((v1, v2)) = nodeweight(child) * shortest_path(parent) / allparents.toDouble
              }
            }
          }
        }
      }
      return between.transform((_, value) => value / 2)
    }

    def maxBetweeness(): (String, String) = {
        return Betweeness().maxBy(_._2)._1
    }

//    def maxBetweeness(): Array[(String, String)] = {
//      val Betweenn = Betweeness()
//      val maxB = Betweenn.maxBy(_._2)
//      Betweenn.filter(_._2 == maxB)
//      return Betweenn.filter(_._2 == maxB).keySet.toArray
//    }

    def Connectivity(): ArrayBuffer[collection.mutable.Set[String]] = {
      val community = new ArrayBuffer[collection.mutable.Set[String]]
      var vertices = getVertices().to[collection.mutable.Set]

      while (vertices.nonEmpty) {
        val connectNodes = BFS(vertices.iterator.next()).keySet.to[collection.mutable.Set]
        community += connectNodes
        vertices --= connectNodes
      }
      return community
    }

    def Modularity(): Double = {
      var modu = 0
      val m2 = get2Edges()
      for (com <- Connectivity()) {
        if (com.size != 1) {
          var a = 0
          var k = 0

          while (com.nonEmpty) {
            val i = com.iterator.next()
            com -= i
            a += (originalG(i) & com).size
            com.foreach(k += getDegree(_) * getDegree(i))
          }
          modu += a - (k / m2)
        }
      }
      return modu
    }

    def detectCommunity(): ArrayBuffer[collection.mutable.Set[String]] = {

      while (adjacentDict.nonEmpty) {
        val modu = Modularity()
        //        println(adjacentDict)
        if (modu > maxModularity) {
          maxModularity = modu
          Community = Connectivity()
        }
        val delE = maxBetweeness()
//        delE.foreach(e => deleteEdge(e._1, e._2))
        deleteEdge(delE._1, delE._2)
      }
      return Community
    }
  }

  def main(args: Array[String]){

    val Start = System.nanoTime

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("hw")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)

    val threshold = args(0).toInt
    val inputFilePath = args(1)
    val outputFilePathb = args(2)
    val outputFilePathc = args(3)

    val inputFile = sc.textFile(inputFilePath)
    val header = inputFile.first()
    val inputRDD = inputFile.filter(row => row!=header).
      map(_.split(",")).map(row => (row(0), row(1))).persist(StorageLevel.DISK_ONLY)

    val edgesRDD = inputRDD.map(row => (row._2, row._1)).groupByKey.map(_._2.toArray.sorted).
       flatMap(_.combinations(2).map{ case Array(p1, p2) => ((p1, p2), 1)}).reduceByKey(_+_).
       filter(_._2 >= threshold).flatMap(edge => Array(edge._1, (edge._1._2, edge._1._1)))

//    val vertices = edgesRDD.map(edge => edge._1).distinct
    val adjacentMap = edgesRDD.groupByKey.mapValues(_.to[collection.mutable.Set]).collect.toMap

    val graph = new Graph(adjacentMap)

//    Betweenness
    val between = graph.Betweeness()
    val outputB = between.toSeq.map(e => (e._1.productIterator.toList.mkString("('", "', '", "')"), e._2)).sortBy(e => (-e._2, e._1)).
      map(e => e._1 + ", " + e._2.toString).mkString("\n")
    val writer = new PrintWriter(new File(outputFilePathb))
    writer.write(outputB)
    writer.close()

//    Community
    val community = graph.detectCommunity()
    println(graph.maxModularity)
    println(graph.Community.size)

    val outputC = community.map(_.toList.sorted.mkString("'", "', '", "'")).sortBy(str => (str.length, str)).mkString("\n")
    val writer2 = new PrintWriter(new File(outputFilePathc))
    writer2.write(outputC)
    writer2.close()

    val end = System.nanoTime
    println("Duration: " + ((end-Start)/1000000000.0).toInt + " seconds" )
  }
}
