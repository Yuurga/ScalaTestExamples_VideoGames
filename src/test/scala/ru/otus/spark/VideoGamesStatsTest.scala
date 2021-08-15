package ru.otus.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, PrivateMethodTester}

class VideoGamesStatsTest extends FunSuite with BeforeAndAfterAll with PrivateMethodTester {

  @transient var ss: SparkSession = _

  override def beforeAll(): Unit = {
    ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("VideoGamesStatsTest")
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

  }

  override def afterAll(): Unit = {
    ss.stop()
  }

  test("test Load") {
    val loadMethod = PrivateMethod[DataFrame]('loadData)
    val res = VideoGamesStats invokePrivate loadMethod(ss)

    assert(!res.isEmpty, "We load some data, dataframe is not empty")
    assertResult(16598) {
      res.count()
    }

  }


}
