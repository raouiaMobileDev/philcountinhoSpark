package com.databeans.main

import com.databeans.gold.MostCommentedPostPerUser.extractMostCommentedPostPerUser
import com.databeans.silver.ExtractPostInfoData.extractPostInfoData
import org.apache.spark.sql.SparkSession

object MostCommentedPostPerUserMain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Main")
      .getOrCreate()
    import spark.implicits._

    val philCoutinhoData = spark.read.option("multiline","true").json("phil.coutinho-1.json")
    val postInfoData = extractPostInfoData(spark,philCoutinhoData)
    val mostCommentedPostPerUserData = extractMostCommentedPostPerUser(spark, postInfoData)
    mostCommentedPostPerUserData.write.mode("overwrite").saveAsTable("MostCommentedPostPerUser")
  }
}
