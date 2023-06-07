package main

import com.databeans.gold.LikeEvolutionWithTime.extractLikeEvolutionWithTime
import com.databeans.gold.MostCommentedPostPerUser.extractMostCommentedPostPerUser
import com.databeans.gold.MostLikedPostPerUser.extractMostLikedPostPerUser
import com.databeans.gold.TopTenMonthlyCommentersPerUser.extractTopTenMonthlyCommentersPerUser
import org.apache.spark.sql.SparkSession
import com.databeans.models._


object MainGold {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Test")
      .getOrCreate()
    import spark.implicits._

    val commentData = spark.read.parquet("hdfs/data/silver/Comment")
    val postInfoData = spark.read.parquet("hdfs/silver/PostInfo")

    val likeEvolutionWithTimeData = extractLikeEvolutionWithTime(spark,postInfoData)
    likeEvolutionWithTimeData.write.mode("overwrite").format("parquet").save("hdfs/data/gold/LikeEvolutionWithTime")

    val mostCommentedPostPerUserData = extractMostCommentedPostPerUser(spark, postInfoData)
    mostCommentedPostPerUserData.write.mode("overwrite").format("parquet").save("hdfs/data/gold/MostCommentedPostPerUser")

    val mostLikedPostPerUserData = extractMostLikedPostPerUser(spark, postInfoData)
    mostLikedPostPerUserData.write.mode("overwrite").format("parquet").save("hdfs/data/gold/MostLikedPostPerUser")


    val topTenMonthlyCommentersPerUserData = extractTopTenMonthlyCommentersPerUser(spark, commentData)
    topTenMonthlyCommentersPerUserData.write.mode("overwrite").format("parquet").save("hdfs/data/gold/TopTenMonthlyCommentersPerUser")

  }

}




