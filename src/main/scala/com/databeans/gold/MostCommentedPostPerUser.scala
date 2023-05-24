package com.databeans.gold
import org.apache.spark.sql.{DataFrame, SparkSession}

object MostCommentedPostPerUser{
  def extractMostCommentedPostPerUser(spark:SparkSession, postData: DataFrame): DataFrame={
    postData.createOrReplaceTempView("posts")
    spark.sql(
      """
        |select
        |profile_id,
        |post_id,
        |MAX(edge_media_to_comment) AS mostCommentedPostPerUser
        |from posts
        |group by profile_id, post_id
        |""".stripMargin)
  }
}