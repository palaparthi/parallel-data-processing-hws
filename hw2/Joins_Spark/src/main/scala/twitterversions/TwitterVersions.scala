package twitterversions

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TwitterVersions {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    // Create Spark Context
    val conf = new SparkConf().setAppName("Twitter Versions")
    val sc = new SparkContext(conf)

    // create spark session
    val spark = SparkSession
      .builder()
      .appName("Twitter Versions")
      .getOrCreate()

    import spark.implicits._

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    } catch {
      case _: Throwable => {}
    }
    // ================

    case class Twitter(follower: String, followee: String)
    val textFile = sc.textFile(args(0))
    //val ds = spark.read.option("inferSchema", "true").csv(args(0))

    //ds.show(5)
    // map1 -  Returns list of elements, split with ',' and taking the last element i.e followee
    // map 2 - emit followee as key and 1 as value
    // reduceByKey - group by key and add all the partial values and emit(followee, no of followers)

    // RDD-G
        val counts = textFile.map(line => line.split(",").last)
          .map(followee => (followee, 1))
          .groupByKey
          .map(t => (t._1, t._2.sum))
      logger.info(counts.toDebugString)

    //RDD-R
//        val counts = textFile.map(line => line.split(",").last)
//          .map(followee => (followee, 1))
//          .reduceByKey(_+_)
//        logger.info(counts.toDebugString)

    //RDD-F
//        val counts = textFile.map(line => line.split(",").last)
//          .map(followee => (followee, 1))
//          .foldByKey(0)(_ + _)
//        logger.info(counts.toDebugString)

    //RDD-A
//        val counts = textFile.map(line => line.split(",").last)
//          .map(followee => (followee, 1))
//          .aggregateByKey(0 )(_+_, _+_)
//        logger.info(counts.toDebugString)

    // DSET

    //val counts = ds.groupBy("_c1").count().explain(true)



    //logger.info("RDD INFO " + counts.toDebugString)
    counts.saveAsTextFile(args(1))
    counts.saveAsTextFile(args(1))
  }
}