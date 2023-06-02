package com.databeans

import com.databeans.gold.LikeEvolutionWithTime.extractLikeEvolutionWithTime
import com.databeans.gold.MostCommentedPostPerUser.extractMostCommentedPostPerUser
import com.databeans.gold.MostLikedPostPerUser.extractMostLikedPostPerUser
import com.databeans.gold.TopTenMonthlyCommentersPerUser.extractTopTenMonthlyCommentersPerUser
import org.apache.spark.sql.SparkSession
import com.databeans.models._
import com.databeans.silver.ExtractCommentData.extractCommentData
import com.databeans.silver.ExtractPostInfoData.extractPostInfoData
import com.databeans.silver.ExtractProfileInfoData.extractProfileInfoData

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Test")
      .getOrCreate()
    import spark.implicits._

    val philCoutinhoData = spark.read.option("multiline","true").json("phil.coutinho-1.json")
    val commentData = extractCommentData(spark,philCoutinhoData)
    commentData.write.mode("overwrite").partitionBy("commenter_id").format("parquet").save("hdfs/data/Comment")

    val postInfoData = extractPostInfoData(spark, philCoutinhoData)
    postInfoData.write.mode("overwrite").partitionBy("profile_id").format("parquet").save("hdfs/data/PostInfo")


    val profileInfo = extractProfileInfoData(spark, philCoutinhoData)
    profileInfo.write.mode("overwrite").partitionBy("id").format("parquet").save("hdfs/data/ProfileInfo")

    val likeEvolutionWithTimeData = extractLikeEvolutionWithTime(spark,postInfoData)
    likeEvolutionWithTimeData.write.mode("overwrite").partitionBy("profile_id").format("parquet").save("hdfs/data/LikeEvolutionWithTime")

    val mostCommentedPostPerUserData = extractMostCommentedPostPerUser(spark, postInfoData)
    mostCommentedPostPerUserData.write.mode("overwrite").partitionBy("profile_id").format("parquet").save("hdfs/data/MostCommentedPostPerUser")

    val mostLikedPostPerUserData = extractMostLikedPostPerUser(spark, postInfoData)
    mostLikedPostPerUserData.write.mode("overwrite").partitionBy("profile_id").format("parquet").save("hdfs/data/MostLikedPostPerUser")

    val topTenMonthlyCommentersPerUserData = extractTopTenMonthlyCommentersPerUser(spark, commentData)
    topTenMonthlyCommentersPerUserData.write.mode("overwrite").partitionBy("profile_id").format("parquet").save("hdfs/data/TopTenMonthlyCommentersPerUser")

  }

}
