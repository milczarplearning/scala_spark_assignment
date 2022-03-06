package com.data.engineer.assignment

import com.data.engineer.assignment.utils.IOUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Part3 {

  private val DATASET_PATH = "src/main/resources/part_3/iris.csv"
  private val featureColumnNames = Seq("sepal_length", "sepal_width", "petal_length", "petal_width")
  val origLabelColumnName = "class"

  val modelLabelName = "label"
  val modelFeaturesColumnName = "features"

  def solveTasks(spark: SparkSession): Unit = {
    val trainingDataFrame = loadDataFrame(spark)
    val model = getPipelineModel(trainingDataFrame)
    val classToLabelMapping = model.transform(trainingDataFrame)
      .select(origLabelColumnName, modelLabelName)
      .distinct()

    import spark.implicits._
    val predData = Seq(
      (5.1, 3.5, 1.4, 0.2),
      (6.2, 3.4, 5.4, 2.3)
    ).toDF(featureColumnNames: _*)

    val predictions = model.transform(predData)
      .join(classToLabelMapping, 'prediction === col(modelLabelName))

    IOUtils.writeArrayToFile(predictions.select(origLabelColumnName))
  }

  private def loadDataFrame(spark: SparkSession) = {
    val schema = StructType(
      featureColumnNames.map(columnName => StructField(columnName, DoubleType, true)) :+
        StructField(origLabelColumnName, StringType, true)
    )
    val trainigDataFrame = spark.read.schema(schema).csv(DATASET_PATH)
    trainigDataFrame
  }

  def getPipelineModel(trainingDataFrame: DataFrame) = {
    val indexer = new StringIndexer()
      .setInputCol(origLabelColumnName)
      .setOutputCol(modelLabelName)
      .setHandleInvalid("keep")
    val assembler = new VectorAssembler()
      .setInputCols(featureColumnNames.toArray)
      .setOutputCol("features")
    val logisticRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(100000.0)
      .setTol(0.0001)
      .setFeaturesCol("features")
      .setLabelCol(modelLabelName)

    val stages = Array(assembler, indexer, logisticRegression)
    val pipeline = new Pipeline().setStages(stages)
    val pipelineModel = pipeline.fit(trainingDataFrame)
    pipelineModel
  }
}