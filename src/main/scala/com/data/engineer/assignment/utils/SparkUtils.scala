package com.data.engineer.assignment.utils

import org.apache.spark.sql._

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

  def writeDataFrameCsvTxt(path: String, df: Dataset[Row], header: Boolean = true): Unit = {
    val rows = df.collect()
      .map(row => row.toSeq.mkString(","))
      .mkString("\n")
    val result = if (header) StringBuilder.newBuilder.append(df.columns.mkString(","))
      .append("\n")
      .append(rows)
      .result()
    else
      rows

    IOUtils.writeToFile(path, result)
  }

}
