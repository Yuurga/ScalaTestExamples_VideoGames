package ru.otus.spark.sales

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSessionTestWrapper extends BeforeAndAfterAll {
  self: Suite =>
  @transient private var _ss: SparkSession = _

  def ss: SparkSession = _ss

  var conf = new SparkConf(false)

  override def beforeAll() {
    _ss =     SparkSession.builder()
      .master("local[*]")
      .appName("spark session")
      .enableHiveSupport()
      .getOrCreate()
    _ss.sparkContext.setLogLevel("WARN")
    super.beforeAll()
  }

  override def afterAll() {
    _ss.stop()
    super.afterAll()
  }


}
