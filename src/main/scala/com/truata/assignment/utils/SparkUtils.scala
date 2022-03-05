package com.truata.assignment.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSpark: SparkSession = SparkSession
    .builder()
    .appName("TruataAssignment")
    .master("local")
    .getOrCreate()


}
