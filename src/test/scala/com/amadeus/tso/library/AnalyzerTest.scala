package com.amadeus.tso.library

import com.amadeus.tso.library.sample._
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

case class CustomEvent(timestamp: java.sql.Timestamp, office: String, userId: String, action: String, container: String)

class AnalyzerTest extends FlatSpec with Matchers {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("UnitTest")
    .config("spark.sql.caseSensitive", "true")
    .getOrCreate()

  /**
    * In this example, the client stored time series as a dataframe and wants to extract anomalies.
    */
  "CustomAnalyzer" should "analyze time series and identify anomalies" in {
    val timeSeries = spark.read.json("src/test/resources/time_series_sample_1.json")
    val profileMap = spark.read.option("header", "true").csv("src/test/resources/profiles_sample_1.csv")
    val referenceModel = spark.read.option("header", "true").csv("src/test/resources/reference_sample_1.csv")

    val anomalies = new CustomAnalyzer().detectAnomalies(profileMap, referenceModel)(timeSeries)

    val expected = spark.read.json("src/test/resources/anomalies_sample_1.json")
    anomalies.toJSON.collect.map(_.parseJson) should contain theSameElementsAs expected.toJSON.collect.map(_.parseJson)
  }

  /**
    * In this test, the client wants to transform events to time series and extract anomalies in one go,
    * without saving time series as intermediate result.
    */
  "Full chain" should "extract anomalies from events" in {
    val events = spark.read.option("header", "true").csv("src/test/resources/events_sample_1.csv")
    val profileMap = spark.read.option("header", "true").csv("src/test/resources/profiles_sample_1.csv")
    val referenceModel = spark.read.option("header", "true").csv("src/test/resources/reference_sample_1.csv")

    val anomalies = events
      .transform(new CustomAggregator().aggregate)
      .transform(new CustomAnalyzer().detectAnomalies(profileMap, referenceModel))

    val expected = spark.read.json("src/test/resources/anomalies_sample_1.json")
    anomalies.toJSON.collect.map(_.parseJson) should contain theSameElementsAs expected.toJSON.collect.map(_.parseJson)
  }
}
