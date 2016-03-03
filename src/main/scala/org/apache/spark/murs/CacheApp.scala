package org.apache.spark.murs

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-19.
 */
object CacheApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CacheApp").set("spark,murs.samplingInterval", args(0))
    val sparkContext = new SparkContext(sparkConf)

    val firstRDD = sparkContext.textFile(args(1), args(2).toInt)
    val cacheRDD = firstRDD.map(line => {
      val parts = line.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }).cache()

    val cacheNewRDD = cacheRDD.mapValues((_ * 2))

    val saveRDD = cacheRDD.union(cacheNewRDD)

    saveRDD.saveAsTextFile(args(3))
  }

}
