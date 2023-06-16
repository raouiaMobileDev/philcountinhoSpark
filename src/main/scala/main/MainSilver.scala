
package main
import org.apache.spark.sql.SparkSession
import com.databeans.silver.ExtractCommentData.extractCommentData
import com.databeans.silver.ExtractPostInfoData.extractPostInfoData
import com.databeans.silver.ExtractProfileInfoData.extractProfileInfoData

object MainSilver {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Test")
      .getOrCreate()

    val philCoutinhoData = spark.read.parquet("hdfs/data/bronze/PhilCoutinho")

    val commentData = extractCommentData(spark, philCoutinhoData)
    commentData.write.mode("overwrite").format("parquet").save("hdfs/data/silver/Comment")

    val postInfoData = extractPostInfoData(spark, philCoutinhoData)
    postInfoData.write.mode("overwrite").format("parquet").save("hdfs/data/silver/PostInfo")


    val profileInfo = extractProfileInfoData(spark, philCoutinhoData)
    profileInfo.write.mode("overwrite").format("parquet").save("hdfs/data/silver/ProfileInfo")

  }
}