package com.databeans.bronze

import org.apache.spark.sql.{DataFrame, SparkSession}

object BronzeLayerIngestion {
  def loadIntoBronzeLayer(spark:SparkSession, inputPath: String): Unit = {
    val inputPhilCoutinhoData = spark.read.option("multiline","true").json(inputPath)
    inputPhilCoutinhoData.write.mode("append").format("parquet").save("hdfs/data/bronze/PhilCoutinho")

  }
}