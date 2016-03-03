package org.apache.spark.murs

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-19.
 */
object ReduceByKeyApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ReduceByKeyApp").set("spark,murs.samplingInterval", args(0))
    val sparkContext = new SparkContext(sparkConf)

    val firstRDD = sparkContext.textFile(args(1), args(2).toInt)
    val reduceRDD = firstRDD.flatMap( line => {
      val parts = line.split("\\s+")
      parts.map(Integer.parseInt(_)).map((_,1))
    }).reduceByKey(_ + _)

    reduceRDD.saveAsTextFile(args(3))
  }

}
