package ru.otus.spark

import org.apache.spark.sql.types.{DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.otus.spark.sales.BiggestSalesCalculation

object VideoGamesStats {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("VideoGamesStats")
      .getOrCreate()

    println(ss.version)

    val in = loadData(ss)

    val sales = in.withColumn("Global_Sales_int", in("Global_Sales").cast(DoubleType))
      .drop("Global_Sales")
      .withColumnRenamed("Global_Sales_int", "Global_Sales")


    sales.show()


    val bigSalesCalc = new BiggestSalesCalculation(ss)
    bigSalesCalc.sumSalesByPublisher(sales)

  }

  private def loadData(ss: SparkSession): DataFrame = ss.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("src/main/resources/vgsales.csv")


}
