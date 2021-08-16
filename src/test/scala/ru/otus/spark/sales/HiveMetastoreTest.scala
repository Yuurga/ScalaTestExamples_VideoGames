package ru.otus.spark.sales

import org.apache.spark.sql.SaveMode
import org.scalatest.FunSuite

class HiveMetastoreTest extends FunSuite with SparkSessionTestWrapper {

  test("Save dataframe with final fantasy and load from table all 89 records") {
    val df = ss.read
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/vgsales.csv")

    val ffDf = df.filter(df("Name").contains("Final Fantasy"))

    ffDf.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .saveAsTable("ff_stats")

    val newDf = ss.sql("select * from ff_stats")
    newDf.show()

    assertResult(89) {
      newDf.count()
    }

  }

}
