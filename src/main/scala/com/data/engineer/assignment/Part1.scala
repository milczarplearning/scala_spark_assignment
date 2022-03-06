package com.data.engineer.assignment

import com.data.engineer.assignment.utils.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Part1 {

  private val DATASET_PATH = "src/main/resources/part_1/groceries.csv"

  def solveTasks(sc: SparkContext): Unit = {
    val lines = sc.textFile(DATASET_PATH)

    // Task 2
    val products = lines.flatMap(_.split(","))
    val uniqueProducts = products.distinct().cache()
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
