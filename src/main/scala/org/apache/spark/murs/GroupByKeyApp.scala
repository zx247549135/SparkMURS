package org.apache.spark.murs

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-19.
 */
object GroupByKeyApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GroupByKeyApp").set("spark,murs.samplingInterval", args(0))
    val sparkContext = new SparkContext(sparkConf)

    val firstRDD = sparkContext.textFile(args(1), args(2).toInt)
    val groupRDD = firstRDD.map(line => {
      val parts = line.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }).groupByKey()

    groupRDD.map( line => {
      (line._1, line._2.toArray.mkString(","))
    }).saveAsTextFile(args(3))
  }

}
