package com.databeans.silver

import com.databeans.silver.ExtractPostInfoData.extractPostInfoData
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.databeans.models._
case class ResultPostData(profile_id:String, post_id:String, typename:String, comments_disabled:Boolean, edge_media_preview_like:Long, edge_media_to_comment: Long, is_video: Boolean, taken_at_timestamp:Long, username:String)

class ExtractPostDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("CountinhoDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "ExtractCoutinhoData" should "Extract coutinho data within the DataFrame" in {
    Given("The input nasted dataframe")
   val inputCommentsData= Comments(Seq(
     Data(1619023963, "18209883163069294", Owner("20740995","sergiroberto"), "💪🏼💪🏼")
   ))
    val inputGraphImagesData= Seq(
      GraphImages("GraphImage", inputCommentsData, false, Edge_media_preview_like(483475),Edge_media_to_comment(80),
        "2556864304565671217", false,OwnerGraphImages("1382894360"),"CN7zonEg1Ux", 1619021998, "phil.coutinho")
    )
    val inputData = Seq(
      InputData(inputGraphImagesData, GraphProfileInfo(1286323200, Info("",23156762,1092, "Philippe Coutinho", "1382894360", false, false, false, 618),"phil.coutinho"))
    ).toDF()
    When("ExtractPostData is invoked")
    val resultPostInfoData = extractPostInfoData(spark,inputData)
    Then("The dataframe should be returned")
    val expectedResultPostData = Seq(
      ResultPostData("1382894360", "2556864304565671217", "GraphImage", false, 483475, 80, false, 1619021998, "phil.coutinho"),
    ).toDF()
    expectedResultPostData.collect() should contain theSameElementsAs (resultPostInfoData.collect())
  }
}
