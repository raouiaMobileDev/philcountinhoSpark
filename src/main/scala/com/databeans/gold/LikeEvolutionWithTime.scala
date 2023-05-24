package com.databeans.gold
import org.apache.spark.sql.{DataFrame, SparkSession}

object LikeEvolutionWithTime {
  def extractLikeEvolutionWithTime (spark: SparkSession, postData: DataFrame): DataFrame= {
    postData.createOrReplaceTempView("posts") // Only allowed when you want to AGG directly from dataframe
   spark.sql(
      """
        |select
        |profile_id,
        |SUM(edge_media_preview_like) as sum,
        |FROM_UNIXTIME(taken_at_timestamp/1000) AS day
        |from posts
        |group by profile_id, day
        |""".stripMargin)

  }
}

