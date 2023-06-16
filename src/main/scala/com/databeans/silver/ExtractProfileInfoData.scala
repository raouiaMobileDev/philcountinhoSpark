package com.databeans.silver

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractProfileInfoData {
  def extractProfileInfoData(spark:SparkSession,inputData: DataFrame): DataFrame = {

    val extractedProfileInfoData=inputData.select(
      col("GraphProfileInfo.info.id"),
      col("GraphProfileInfo.info.followers_count"),
      col("GraphProfileInfo.info.following_count"),
      col("GraphProfileInfo.info.full_name"),
      col("GraphProfileInfo.info.is_business_account"),
      col("GraphProfileInfo.info.is_private"),
      col("GraphProfileInfo.info.posts_count"),
      col("GraphProfileInfo.username"),
    )
    extractedProfileInfoData

  }
}
