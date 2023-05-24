package com.databeans.main

import com.databeans.silver.ExtractProfileInfoData.extractProfileInfoData
import org.apache.spark.sql.SparkSession

object ExtractProfileInfoDataMain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Main")
      .getOrCreate()
    import spark.implicits._

    val philCoutinhoData = spark.read.option("multiline","true").json("phil.coutinho-1.json")
    val profileInfo = extractProfileInfoData(spark, philCoutinhoData)
    profileInfo.write.mode("overwrite").saveAsTable("ProfileInfo")
  }
}
