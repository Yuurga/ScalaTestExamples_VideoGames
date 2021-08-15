package ru.otus.spark.sales

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class BiggestSalesCalculation(@transient val ss: SparkSession) {


  def sumSalesByPublisher(df: DataFrame): DataFrame = {
    if (df.isEmpty) throw new Exception("No data to calculate")

    val sumSales = df.groupBy("Publisher").sum("Global_Sales")
      .orderBy(desc("sum(Global_Sales)"))

    sumSales.show(5)

    sumSales
  }


}
