package com.data.engineer.assignment.parts

import com.data.engineer.assignment.utils.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Part1(sc: SparkContext, datasetPath: String) {

  def solveTasks(): Unit = {
    val lines = sc.textFile(datasetPath)

    // Task 2
    val products = lines.flatMap(_.split(","))
    val uniqueProducts = products.distinct().cache()  // I added here cache because if we had multiple csv files the loading and processing time would have an impact
                                                      // for that particular dataset it doesn't have impact
    IOUtils.writeArrayToFile("out/out_1_2a.txt", uniqueProducts.collect())

    val uniqueCount = uniqueProducts.count()
    IOUtils.writeToFile("out/out_1_2b.txt", s"Count:\n $uniqueCount")

    // Task 3
    val top5products = takeTop5products(products)
    IOUtils.writeArrayToFile("out/out_1_3.txt", top5products)

  }

  private def takeTop5products(products: RDD[String]) = {
    products.map(product => (product, 1))
      .reduceByKey(_ + _)
      .top(5)(Ordering.by(_._2))
      .map(pair =>s"('${pair._1}', ${pair._2})")
  }
}
