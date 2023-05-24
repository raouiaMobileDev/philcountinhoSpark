package com.databeans

import org.apache.spark.sql.SparkSession
import com.databeans.models._
import com.databeans.silver.ExtractPostInfoData.extractPostInfoData

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Test")
      .getOrCreate()
    import spark.implicits._

    val inputCommentsData = Comments(Seq(
      Data(1619023963, "18209883163069294", Owner("20740995","sergiroberto"), "💪🏼💪🏼"),
      Data(1619023963, "18209883163069294", Owner("20740995","sergiroberto"), "hhhhhhhh")

    ))
    val inputGraphImagesData= Seq(
      GraphImages("GraphImage", inputCommentsData, false, Edge_media_preview_like(483475),Edge_media_to_comment(80),
        "2556864304565671217", false,OwnerGraphImages("1382894360"),"CN7zonEg1Ux", 1619021998, "phil.coutinho")
    )
    val inputData = Seq(
      InputData(inputGraphImagesData, GraphProfileInfo(1286323200, Info("",23156762,1092, "Philippe Coutinho", "1382894360", false, false, false, 618),"phil.coutinho"))
    ).toDF()
    val resultPostData = extractPostInfoData(spark,inputData)
   // val mostLikedPostPerUser = extractLikeEvolutionWithTime(spark,resultPostData)
   // mostLikedPostPerUser.show()
    // Register the DataFrame as a table in the metastore
    //mostLikedPostPerUser.write.mode("overwrite").saveAsTable("my_table")

    // val resultCommentData = extractCommentData(spark,inputData)
    // val mostLikedCommentPerUser = extractTopTenMonthlyCommentersPerUser(spark,resultCommentData)

  }

}
