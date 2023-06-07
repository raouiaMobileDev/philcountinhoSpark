
package com.databeans

import com.databeans.bronze.philCoutinhoData
import org.apache.spark.sql.SparkSession

object mainBronze  {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CountinhoDataFrame_Test")
      .getOrCreate()
    import spark.implicits._

    val inputPhilCoutinhoData=philCoutinhoData.inputtData(spark,"phil.coutinho-1.json")
    inputPhilCoutinhoData.write.mode("overwrite").format("parquet").save("hdfs/data/bronze/PhilCoutinho")


  }

}

