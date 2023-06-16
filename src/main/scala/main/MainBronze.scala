package com.databeans
import com.databeans.bronze.BronzeLayerIngestion.loadIntoBronzeLayer
import org.apache.spark.sql.SparkSession

object MainBronze{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Test")
      .getOrCreate()

    loadIntoBronzeLayer(spark, "phil.coutinho-1.json")
  }
}

