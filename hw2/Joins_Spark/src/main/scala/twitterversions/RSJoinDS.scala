package twitterversions

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object RSJoinDS {
  def main(args: Array[String]): Unit = {
    val MAX = 10
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }

    // Create Spark Context
    val conf = new SparkConf().setAppName("RSJoinDS")
    val sc = new SparkContext(conf)

    // create spark session
    val spark : SparkSession = SparkSession.builder()
      .appName("RSJoinDS")
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


    val ds1 = spark.read.option("inferSchema", "true").csv(args(0))
    val ds2 = spark.read.option("inferSchema", "true").csv(args(0))

    val ds3 = ds1.select($"_c0".alias("k1"), $"_c1".alias("v1"))
    val ds4 = ds1.select($"_c0".alias("k2"), $"_c1".alias("v2"))
    //case class JoinOutput(_c0: Int, _c1:Int)

    // Join on 2nd column to find path2 1,2 and 2,3 => 1,3
    val path2DS = ds3.as("S1").join(ds4.as("S2")).where($"S1.v1" === $"S2.k2")
    // find triangle with original dataset 1,3 and 3,1 => 1 triangle formed
    val triangle = path2DS.as("S1").join(ds3.as("S2")).where($"S1.v2" === $"S2.k1" and $"S1.k1" === $"S2.v1").count()


    logger.info("Triangle Count" + triangle)
    val triangleCountRdd= sc.parallelize(Seq(triangle/3))
    triangleCountRdd.saveAsTextFile(args(1))
    logger.info("Number of Triangles " + triangle/3);
  }
}
