package com.databeans.main

import com.databeans.silver.ExtractCommentData.extractCommentData
import org.apache.spark.sql.SparkSession

object ExtractCommentDataMain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Main")
      .getOrCreate()
    import spark.implicits._

    val philCoutinhoData = spark.read.option("multiline","true").json("phil.coutinho-1.json")
    val commentData = extractCommentData(spark,philCoutinhoData)
    //commentData.write.partitionBy("id").parquet("/data/mydata2")
    commentData.write.mode("overwrite").format("parquet").saveAsTable("Comment")
  }
}
