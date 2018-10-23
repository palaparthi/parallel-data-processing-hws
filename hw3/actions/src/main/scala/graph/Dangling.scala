package graph

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object Dangling {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val k = 100

    // Create Spark Context
    val conf = new SparkConf().setAppName("Dangling")
    val sc = new SparkContext(conf)

    val graphList = (1 to k * k).map(e =>
      if (e % k != 0) (e, e + 1) else (e, 0))
    var prList = (1 to k * k).map(e => (e, 1.0d / (k * k).toDouble))
    prList = prList :+ (0, 0d)
    val startList = (1 to k * k).filter(s => s % k == 1).map(s => (s, 0d))

    val graph = sc.parallelize(graphList, 2)
    var pr = sc.parallelize(prList, 2)
    val startListRdd = sc.parallelize(startList)
    graph.persist()
    var delta = 0d

    for (i <- 1 to 10) {

      // join graph and pr, add back the nodes with no incoming edges, since they are lost after map
      val joinTables = graph.join(pr)
        .map(e => e._2)
        .reduceByKey(_ + _)
        .union(startListRdd)
      delta = joinTables.lookup(0)(0)

      // Distribute delta to all k^2 nodes excluding the dummy 0
      val newPR = joinTables.filter(f => f._1 != 0)
        .map(pr => (pr._1, pr._2 + delta / (k * k).toDouble))
      pr = newPR
      val sum = newPR.map(_._2).sum()
      logger.info("sumi " + i + " " + sum)
    }
    logger.info("Final Delta" + delta)
    val sorted = pr.sortBy(_._1)
    sorted.collect().foreach(println)
  }
}