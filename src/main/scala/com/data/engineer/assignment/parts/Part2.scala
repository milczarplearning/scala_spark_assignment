package com.data.engineer.assignment.parts

import com.data.engineer.assignment.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class Part2(spark: SparkSession, datasetPath: String) {


  def solveTasks(): Unit = {

    val df = spark.read.parquet(datasetPath)

    val task2Result = df.select(
      min("price").as("min_price"),
      max("price").as("max_price"),
      count("price").as("row_count")
    )

    SparkUtils.writeDataFrameCsvTxt("out/out_2_2.txt", task2Result)


    import spark.implicits._
    val task3Result = df.where('price > 5000.0 and 'review_scores_value === 10.0)
      .select(
        avg("bathrooms").as("average_bathrooms_count"),
        avg("bedrooms").as("average_bedrooms_count"),
      )

    SparkUtils.writeDataFrameCsvTxt("out/out_2_3.txt", task3Result)


    // How many people can be accommodated by the property with the lowest price and highest rating?
    val windowSpec = Window.orderBy('price, 'review_scores_rating.desc)
    val task4Result = df.withColumn("rank", dense_rank().over(windowSpec))
      .where('rank === 1)
      .select(sum('accommodates))

    SparkUtils.writeDataFrameCsvTxt("out/out_2_4.txt", task4Result, header = false)

  }
}
