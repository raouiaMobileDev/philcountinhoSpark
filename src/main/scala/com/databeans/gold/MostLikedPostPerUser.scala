package com.databeans.gold
import org.apache.spark.sql.{DataFrame, SparkSession}

object MostLikedPostPerUser {
  def extractMostLikedPostPerUser(spark:SparkSession, postData: DataFrame):DataFrame={
    postData.createOrReplaceTempView("posts")
     spark.sql(
    """
      |select
      |profile_id,
      |post_id,
      |MAX(edge_media_preview_like) AS mostLikedPostPerUser
      |from posts
      |group by profile_id, post_id
      |""".stripMargin)
  }
}
