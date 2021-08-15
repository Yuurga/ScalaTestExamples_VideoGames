package ru.otus.spark.sales

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val ss: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("spark session")
      .getOrCreate()
  }


}
