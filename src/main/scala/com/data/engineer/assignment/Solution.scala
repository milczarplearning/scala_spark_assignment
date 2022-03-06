package com.data.engineer.assignment

import com.data.engineer.assignment.utils.SparkUtils
import org.apache.spark.SparkContext

object Solution {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark
    val sparkContext = spark.sparkContext
    Part1.solveTasks(sparkContext)
    Part2.solveTasks(spark)
    Part3.solveTasks(spark)
  }
}
