package ru.otus.spark.sales

import org.scalatest.FunSuite

class BiggestSalesCalculationTest extends FunSuite  with SparkSessionTestWrapper  {


  test("Should throw exception on empty DataFrame") {
    val  calc = new BiggestSalesCalculation(ss)
    val caught =
      intercept[Exception] {
        calc.sumSalesByPublisher(ss.emptyDataFrame)
      }

    assert(caught.getMessage.eq("No data to calculate"))

  }

}
