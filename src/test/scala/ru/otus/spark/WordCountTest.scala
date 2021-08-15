package ru.otus.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.collection.mutable


class WordCountTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  @transient var ss: SparkSession = _

  override def beforeAll(): Unit = {
    ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCountTest")
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

  }

  override def afterAll(): Unit = {
    ss.stop()
  }

  test("Test Word Count works correct") {
    val textRDD = ss.sparkContext
      .parallelize(Seq("Witches in Pratchett's universe are largely stripped of " +
        "their modern occultist associations (though Pratchett does frequently" +
        " use his stories to lampoon such conceptions of witchcraft), and" +
        " act as herbalists, adjudicators and wise women. Witches on the Disc " +
        "can use magic, but generally prefer not to, finding simple but cunningly " +
        "applied psychology far more effective."))

    val wordCountRDD = textRDD.flatMap(r => r.split(' ')).
      map(r => (r.toLowerCase, 1)).
      reduceByKey((a, b) => a + b)

    val wordMap = new mutable.HashMap[String, Int]()
    wordCountRDD.collect().foreach(r => wordMap.put(r._1, r._2))


    assert(wordMap("witches") == 2, "The word count for 'witches' should had been 2 but it was " + wordMap("witches"))
    //assert(wordMap("use") == 3, "The word count for 'use' should had been 3 but it was " + wordMap("use"))
  }


}
