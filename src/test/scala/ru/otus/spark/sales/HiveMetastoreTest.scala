package ru.otus.spark.sales

import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.FunSuite

class HiveMetastoreTest extends FunSuite with SparkSessionTestWrapper {

  override def beforeAll() {
    super.beforeAll()
    //ss.sqlContext.sql("drop table person")
  }

  test("Test table creation and summing of counts") {
    val personRDD = ss.sparkContext.parallelize(Seq(Row("Anna", 25, "manager"),
      Row("Andrew", 35, "devOps"),
      Row("Alexander", 19, "Junior")))

    ss.sqlContext.sql("create table person (name string, age int, position string)")

    val emptyDataFrame = ss.sqlContext.sql("select * from person limit 0")

    val personDataFrame = ss.sqlContext.createDataFrame(personRDD, emptyDataFrame.schema)
    personDataFrame.createOrReplaceTempView("tempPerson")

    val ageSumDataFrame = ss.sqlContext.sql("select sum(age) from tempPerson")

    personDataFrame.write.mode("overwrite").insertInto("person")

    val localAgeSum = ageSumDataFrame.take(10)

    assert(localAgeSum(0).get(0) == 79, "The sum of age should equal 62 but it equaled " + localAgeSum(0).get(0))
  }


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
