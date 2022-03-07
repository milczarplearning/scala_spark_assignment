package com.data.engineer.assignment.parts

import com.data.engineer.assignment.utils.SparkUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Part3(spark: SparkSession, datasetPath: String) {

  private val FeatureColumnNames = Seq("sepal_length", "sepal_width", "petal_length", "petal_width")
  private val ClassColumnName = "class"

  private val ModelLabelName = "label"
  private val ModelFeaturesColumnName = "features"

  def solveTasks(): Unit = {
    val trainingDataFrame = loadDataFrame(spark, datasetPath)
    val (model, indexer) = getPipelineModelAndLbelIndexer(trainingDataFrame)
    val indexToString = getPredictionIndexToString(trainingDataFrame, indexer)
    import spark.implicits._
    val predData = Seq(
      (5.1, 3.5, 1.4, 0.2),
      (6.2, 3.4, 5.4, 2.3)
    ).toDF(FeatureColumnNames: _*)

    val predictions = predData.transform(model.transform)
      .transform(indexToString.transform)


    SparkUtils.writeDataFrameCsvTxt("out/out_3_2.txt", predictions.select(ClassColumnName))
  }

  private def getPredictionIndexToString(trainingDataFrame: DataFrame, indexer: StringIndexer) = {
    val indexerModel = indexer.fit(trainingDataFrame)
    val indexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol(ClassColumnName)
      .setLabels(indexerModel.labelsArray.head)
    indexToString
  }

  private def loadDataFrame(spark: SparkSession, datasetPath: String) = {
    val schema = StructType(
      FeatureColumnNames.map(columnName => StructField(columnName, DoubleType, false)) :+
        StructField(ClassColumnName, StringType, false)
    )
    val trainigDataFrame = spark.read.schema(schema).csv(datasetPath)
    trainigDataFrame
  }

  private def getPipelineModelAndLbelIndexer(trainingDataFrame: DataFrame) = {
    val indexer = new StringIndexer()
      .setInputCol(ClassColumnName)
      .setOutputCol(ModelLabelName)
      .setHandleInvalid("keep")

    val assembler = new VectorAssembler()
      .setInputCols(FeatureColumnNames.toArray)
      .setOutputCol(ModelFeaturesColumnName)

    val logisticRegression = new LogisticRegression() // honestly I don't know If I set up everything as it was in python version
      .setMaxIter(100)
      .setRegParam(1 / 100000.0)
      .setTol(0.0001)
      .setFeaturesCol(ModelFeaturesColumnName)
      .setLabelCol(ModelLabelName)

    val stages = Array(assembler, indexer, logisticRegression)
    val pipeline = new Pipeline().setStages(stages)
    val pipelineModel = pipeline.fit(trainingDataFrame)

    (pipelineModel, indexer) // returning a tuple is not so clean but I cannot think of anything better
  }
}
