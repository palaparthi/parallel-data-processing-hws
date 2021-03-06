package twitterversions

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object RepJoin {
  def main(args: Array[String]): Unit = {
    val MAX = 1000
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }

    // Create Spark Context
    val conf = new SparkConf().setAppName("Twitter RSJoin")
    val sc = new SparkContext(conf)

    // create spark session
    val spark = SparkSession
      .builder()
      .appName("Twitter RSJoin")
      .getOrCreate()
    val textFile = sc.textFile(args(0))

    import org.apache.spark.SparkContext._
    val textFileB = textFile.sparkContext.broadcast(textFile.collect())

    val splitFile = textFile.map(line => line.split(","))
      .filter(a => (a(0).toInt <= MAX && a(1).toInt <= MAX))
      .map(a => (a(1), a(0)))
    val splitFile2 = textFile.map(line => line.split(","))
      .filter(a => (a(0).toInt <= MAX && a(1).toInt <= MAX))
      .map(a => (a(0), a(1)))

    val joinedPath2 = splitFile.join(splitFile2).map(t => t._2)
    var count = 0
    val paths2 = joinedPath2.collect
    val original = splitFile2.collect
    for (p2 <- paths2) {
      for (o <- original) {
        if (p2._2 == o._1 && p2._1 == o._2) {
          count += 1
        }
      }
    }
    val triangleCountRdd= sc.parallelize(Seq(count/3))
    triangleCountRdd.saveAsTextFile(args(1))
    logger.info("Number of Triangles " + count/3);
  }
}