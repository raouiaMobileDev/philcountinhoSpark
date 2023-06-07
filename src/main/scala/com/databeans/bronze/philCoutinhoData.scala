package com.databeans.bronze

import org.apache.spark.sql.{DataFrame, SparkSession}

object philCoutinhoData {
  def inputtData(spark:SparkSession, inputPath: String): DataFrame = {
    spark.read.option("multiline","true").json(inputPath)
  }
}