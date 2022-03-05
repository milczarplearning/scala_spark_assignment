package com.truata.assignment.utils

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object SparkUtils {

  def getSpark: SparkSession = SparkSession
    .builder()
    .appName("TruataAssignment")
    .master("local")
    .getOrCreate()

  def writeCsv(path: String, df: Dataset[Row]): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
  }

  def writeSingleValue(path: String, df: Dataset[Row]): Unit = {
    df.toString()
  }


}
