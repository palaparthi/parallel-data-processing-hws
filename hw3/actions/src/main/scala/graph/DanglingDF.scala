package graph

import org.apache.log4j.LogManager
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DanglingDF {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val k = 100

    // Create Spark Context
    val conf = new SparkConf().setAppName("DanglingDF")
    val sc = new SparkContext(conf)

    // create spark session
    val spark = SparkSession
      .builder()
      .appName("DanglingDF")
      .getOrCreate()

    import spark.implicits._


    val graphDF = (1 to k * k).map(e =>
      if (e % k != 0) (e, e + 1) else (e, 0)).toDF("V1Gr", "V2Gr")

    var prList = (1 to k * k).map(e => (e, 1.0f / (k * k).toDouble))
    prList = prList :+ (0, 0d)

    var prDF = prList.toDF("V1Pr", "PR")

    val startList = (1 to k * k).filter(s => s % k == 1).map(s => (s, 0d))

    val startDF = startList.toDF("V2Pr", "PR")

    graphDF.persist()
    var delta = 0d

    for (i <- 1 to 10) {
      val temp2 = graphDF.join(prDF, $"V1Gr" === $"V1Pr")
        .select("V2Gr", "PR")
        .groupBy($"V2Gr").sum()
        .select("V2Gr", "sum(PR)")
        .withColumnRenamed("sum(PR)", "PR")
        .withColumnRenamed("V2Gr", "V2Pr")
        .union(startDF)

      delta = temp2.filter("V2Pr == 0").select("PR").collectAsList().get(0).getDouble(0)

      val temp3 = temp2.filter("V2Pr != 0")
        .withColumn("PR", temp2("PR") + delta / (k * k).toDouble)
        .withColumnRenamed("V2Pr", "V1Pr")

      prDF = temp3
      val x = prDF.groupBy().sum("PR").show()
      logger.info("sumi " + i + " " + x)
    }
    logger.info("Final Delta " + delta)
    prDF.sort("V1Pr").show(10000, false)
  }
}