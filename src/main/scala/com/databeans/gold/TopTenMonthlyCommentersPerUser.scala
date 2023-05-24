package com.databeans.gold
import org.apache.spark.sql.{DataFrame, SparkSession}

object TopTenMonthlyCommentersPerUser {
  def extractTopTenMonthlyCommentersPerUser (spark: SparkSession, commentData: DataFrame): DataFrame={
    commentData.createOrReplaceTempView("comments")
    spark.sql(
      """
        |select
        |profile_id,
        |count(commenter_id) as count,
        |MONTH(DATE(FROM_UNIXTIME(created_at/1000))) as month
        |from comments
        |group by profile_id, commenter_id, month
        |ORDER BY month DESC LIMIT 10
        |""".stripMargin)

   // todo: replace with topTenMonthlyCommentersPerUser
  }
}


