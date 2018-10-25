package graph

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object Shortest {
  def getAdjListPairData(line: String): (String, List[(String, Int)]) = {
    val vertexAndList = line.split("\\s+")
    var listVal = vertexAndList(1).split(",").map(v => (v, 1)).toList
    if (vertexAndList(0) == "1") {
      listVal = listVal:+("S", 1)
      return (vertexAndList(0), listVal)
    }
    return (vertexAndList(0), listVal)
  }

  def hasSourceFlag(adjList: List[(String, Int)]): Boolean = {
    val hasSource = adjList.filter(x => x._1 =="S")
    hasSource.nonEmpty
  }

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val infinity = Integer.MAX_VALUE
    // Create Spark Context
    val conf = new SparkConf().setAppName("ShortestPath")
    val sc = new SparkContext(conf)

    // create the graph structure
    val graph = sc.textFile(args(0)).map(line => getAdjListPairData(line))
    graph.persist()
    var distances = graph.mapValues(adjList => hasSourceFlag(adjList) match {
      case true => 0
      case _ => infinity
    })

    for(i <- 1 to 10){
      distances = graph.join(distances).flatMap(x => x._2._1.
      map(l =>  if (x._2._2 == infinity) (l._1, x._2._2) else (l._1, l._2 + x._2._2)):+(x._1, x._2._2))
      .reduceByKey((x,y) => Math.min(x,y))
    }
    var max = Integer.MIN_VALUE
    var node = "0"
    distances.collect().foreach(x => if (x._2 > max) {
      max = x._2
      node = x._1
    })
    val shortestpathRdd= sc.parallelize(Seq(max))
    shortestpathRdd.saveAsTextFile(args(1))
    logger.info("max " + max + " node " + node)
  }
}