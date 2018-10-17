package graph

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object Dangling {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val k = 100
    logger.info("HIII")

    // Create Spark Context
    val conf = new SparkConf().setAppName("Dangling")
    val sc = new SparkContext(conf)

    //    val graph = sc.parallelize(Seq())
    val graphList = (1 to k * k).map(e =>
      if (e % k != 0) (e, e + 1) else (e, 0))
    var prList = (1 to k * k).map(e => (e, 1.0f / (k * k).toFloat))
    prList = prList :+ (0, 0f)
    val startList = (1 to k * k).filter(s => s % k == 1).map(s => (s, 0f))
    logger.info("iefiubdfiuvdfiovla" + startList)

    val graph = sc.parallelize(graphList, 20)
    var pr = sc.parallelize(prList, 20)
    val startListRdd = sc.parallelize(startList)
    pr.persist()
    logger.info("ilakat" + graph.partitions.size + pr.partitions.size)

    for (i <- 1 to 10) {

      // join graph and pr, add back the nodes with no incoming edges, since they are lost after map
      val joinTables = graph.join(pr)
        .map(e => e._2)
        .reduceByKey((x, y) => (x + y))
        .union(startListRdd)
      //joinTables.collect().foreach(println)
      val delta = joinTables.lookup(0)(0)

      // Distribute delta to all k^2 nodes excluding the dummy 0
      val newPR = joinTables.filter(f => f._1 != 0)
        .map(pr => (pr._1, pr._2 + delta / (k * k).toFloat))
      pr = newPR

      val sum = newPR.map(_._2).sum()
      logger.info("delta " + delta + sum)
      newPR.collect().foreach(println)
    }

  }
}