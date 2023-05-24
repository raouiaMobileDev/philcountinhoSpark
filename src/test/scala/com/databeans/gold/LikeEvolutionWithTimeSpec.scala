package com.databeans.gold
import com.databeans.silver.ExtractPostInfoData.extractPostInfoData
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.databeans.gold.LikeEvolutionWithTime.extractLikeEvolutionWithTime
import com.databeans.models._

case class ResultLikeEvolutionWithTimeData(profile_id:String, sum:Long, data:String)

class LikeEvolutionWithTimeSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("CountinhoDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "ExtractCoutinhoData" should "Extract coutinho data within the DataFrame" in {
    Given("The input nasted dataframe")
    // val inputDataa = spark.read.option("multiLine", true).json("phil.coutinho-1.json")
    val inputCommentsData = Comments(Seq(
      Data(1619023963, "18209883163069294", Owner("20740995","sergiroberto"), "💪🏼💪🏼")
    ))
    val inputGraphImagesData= Seq(
      GraphImages("GraphImage", inputCommentsData, false, Edge_media_preview_like(483475),Edge_media_to_comment(80),
        "2556864304565671217", false,OwnerGraphImages("1382894360"),"CN7zonEg1Ux", 1619021998, "phil.coutinho")
    )
    val inputData = Seq(
      InputData(inputGraphImagesData, GraphProfileInfo(1286323200, Info("",23156762,1092, "Philippe Coutinho", "1382894360", false, false, false, 618),"phil.coutinho"))
    ).toDF()

    When("ExtractLikeEvolutionWithTime is invoked")
    val resultPostInfoData = extractPostInfoData(spark,inputData)
    val likeEvolutionWithTimeData = extractLikeEvolutionWithTime(spark,resultPostInfoData)
    Then("The dataframe should be returned")
    val expectedResultLikeEvolutionWithTimeData = Seq(
      ResultLikeEvolutionWithTimeData("1382894360", 483475, "1970-01-19 18:43:41")
    ).toDF()
    expectedResultLikeEvolutionWithTimeData.collect() should contain theSameElementsAs(likeEvolutionWithTimeData.collect())
  }
}
