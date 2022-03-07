package com.data.engineer.assignment

import com.data.engineer.assignment.parts.{Part1, Part2, Part3}
import com.data.engineer.assignment.utils.SparkUtils

object Solution {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark
    val sparkContext = spark.sparkContext
    new Part1(sparkContext, "src/main/resources/part_1/groceries.csv").solveTasks()
    new Part2(spark,"src/main/resources/part_2/airbnb.snappy.parquet").solveTasks()
    new Part3(spark, "src/main/resources/part_3/iris.csv").solveTasks()
  }
}
