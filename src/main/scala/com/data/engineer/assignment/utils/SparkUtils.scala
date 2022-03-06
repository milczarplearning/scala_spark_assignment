package com.data.engineer.assignment.utils

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

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

  def writeSingleValue(path: String, df: DataFrame): Unit = {
    df.toString()
  }

  def writeDataFrameToTxt(path:String, df:DataFrame):Unit = {

  }


}
