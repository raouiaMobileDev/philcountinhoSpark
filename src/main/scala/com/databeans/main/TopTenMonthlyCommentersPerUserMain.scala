package com.databeans.main

import com.databeans.gold.TopTenMonthlyCommentersPerUser.extractTopTenMonthlyCommentersPerUser
import com.databeans.silver.ExtractCommentData.extractCommentData
import org.apache.spark.sql.SparkSession

object TopTenMonthlyCommentersPerUserMain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Main")
      .getOrCreate()
    import spark.implicits._

    val philCoutinhoData = spark.read.option("multiline","true").json("phil.coutinho-1.json")
    val commentData = extractCommentData(spark, philCoutinhoData)
    val topTenMonthlyCommentersPerUserData = extractTopTenMonthlyCommentersPerUser(spark, commentData)
    topTenMonthlyCommentersPerUserData.write.mode("overwrite").saveAsTable("TopTenMonthlyCommentersPerUser")
  }
}
