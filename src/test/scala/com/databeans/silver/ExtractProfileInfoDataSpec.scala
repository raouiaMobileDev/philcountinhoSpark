package com.databeans.silver

import com.databeans.silver.ExtractProfileInfoData.extractProfileInfoData
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.databeans.models._
case class ResultProfileInfoData(id:String, followers_count:Long, following_count: Long, full_name:String, is_business_account:Boolean, is_private:Boolean, posts_count:Long, username:String)

class ExtractProfileInfoDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("CountinhoDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "ExtractCommentData" should "Extract coutinho data within the DataFrame" in {
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
    When("ExtractProfileInfoData is invoked")
    val resultProfileInfoData = extractProfileInfoData(spark,inputData)
    Then("The dataframe should be returned")
    val expectedResultProfileInfoData = Seq(
      ResultProfileInfoData("1382894360", 23156762, 1092, "Philippe Coutinho", false, false, 618,"phil.coutinho"),
    ).toDF()
    expectedResultProfileInfoData.collect() should contain theSameElementsAs (resultProfileInfoData.collect())
  }
}
