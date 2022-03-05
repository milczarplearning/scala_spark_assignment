package com.truata.assignment

import com.truata.assignment.utils.SparkUtils
import org.apache.spark.SparkContext

object Solution {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark
    Part1.solveTasks(spark.sparkContext)
    Part2.solveTasks(spark)

  }
}
