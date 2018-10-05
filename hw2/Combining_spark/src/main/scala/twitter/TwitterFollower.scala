package twitter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, SQLContext}
import org.apache.log4j.LogManager

object TwitterFollower {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    // Create Spark Context
    val conf = new SparkConf().setAppName("Twitter Follower")
    val sc = new SparkContext(conf)

    // create spark session
    val spark = SparkSession
      .builder()
      .appName("Twitter Follower")
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
    //val textFile = spark.read.json(args(0))
    //val textFile: DataFrame = spark.read.csv(args(0))
    val ds = spark.createDataset(textFile)

    ds.show(5)
    // map1 -  Returns list of elements, split with ',' and taking the last element i.e followee
    // map 2 - emit followee as key and 1 as value
    // reduceByKey - group by key and add all the partial values and emit(followee, no of followers)

    // RDD-G
    //    val counts = textFile.map(line => line.split(",").last)
    //      .map(followee => (followee, 1))
    //      .groupByKey
    //      .map(t => (t._1, t._2.sum))

    //RDD-R
    //    val counts = textFile.map(line => line.split(",").last)
    //      .map(followee => (followee, 1))
    //      .reduceByKey(_+_)

    //RDD-F
    //    val counts = textFile.map(line => line.split(",").last)
    //      .map(followee => (followee, 1))
    //      .foldByKey(0)(_ + _)

    //RDD-A
    //    val counts = textFile.map(line => line.split(",").last)
    //      .map(followee => (followee, 1))
    //      .aggregateByKey(0 )(_+_, _+_)

    // DSET

    val counts = ds.map(line => line.split(",").last)
      .map(followee => (followee, 1)).groupBy("_1").avg("_2")
    counts.show(5)
      //counts.show(5)
    //logger.info("RDD INFO " + counts.toDebugString)
    //counts.rdd.saveAsTextFile(args(1))

  }
}