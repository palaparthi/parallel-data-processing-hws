package graph

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ShortestDF {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val infinity = Integer.MAX_VALUE
    val conf = new SparkConf().setAppName("ShortestDF")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("ShortestDF")
      .getOrCreate()

    import spark.implicits._

    var graph = sc.textFile(args(0)).map(line => (line.split(",")(0), line.split(",")(1))).toDS()
    graph.persist()

    var distances = graph.map(x => if (x._1 == "1") (x._1, 0) else (x._1, infinity))
      .withColumnRenamed("_1", "V")
      .withColumnRenamed("_2", "D")
      .distinct()


    for (i <- 1 to 5) {
      val joined = graph.join(distances, $"_1" === $"V").toDF()

      joined.persist()

      // visit neighbors of node with distance other than infinity
      val a = joined
        .filter(x => x.getInt(3) != infinity)
        .select($"_2", $"D" + 1)

      // include nodes with distances as infinity as is
      val b = joined
        .filter(x => x.getInt(3) == infinity)
        .select($"_2", $"D")

      // include node and the shortest distance to get to this node from source
      val c = joined
        .select($"_1", $"D")
        .distinct()
        .withColumnRenamed("_1", "_2")

      // combining all of the above to get updates distances
      distances = a.union(b).union(c).groupBy("_2").min()
          .withColumnRenamed("_2","V")
          .withColumnRenamed("min((D + 1))","D")
    }
    distances.show()
    val max = distances.toDF().sort($"D".desc).first().getInt(1)
    val shortestpathRdd= sc.parallelize(Seq(max))
    shortestpathRdd.saveAsTextFile(args(1))
    logger.info("max " + max)
  }
}