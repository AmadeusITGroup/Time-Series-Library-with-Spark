package com.amadeus.tso.library

import java.io.File

import com.amadeus.tso.library.sample._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class AggregatorTest extends FlatSpec with Matchers {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("UnitTest")
    .config("spark.sql.caseSensitive", "true")
    .getOrCreate()

  /**
    * In this example, the client provides the full implementation of AggregatorTrait in the CustomAggregator class.
    */
  "CustomEventAggregator" should "aggregate time series from custom events" in {
    val events = spark.read.option("header", "true").csv("src/test/resources/events_sample_1.csv")

    val timeSeries = new CustomAggregator().aggregate(events).persist

    val expected = spark.read.json("src/test/resources/time_series_sample_1.json")
    timeSeries.toJSON.collect.map(_.parseJson) should contain theSameElementsAs expected.toJSON.collect.map(_.parseJson)
  }

  /**
    * In this example, the client benefits of the implementation from config inheriting from the Aggregator abstract class.
    */
  "CustomFromConfig" should "read parameters from config and aggregate time series" in {
    val fullConfig = ConfigFactory.parseFile(new File("src/test/resources/sampleAggregator.conf"))
    val aggregatorConfig = fullConfig.getConfig("Aggregator")

    val events = spark.read.option("header", "true").csv("src/test/resources/events_sample_1.csv")

    val timeSeries = new CustomAggregatorFromConfig(aggregatorConfig).aggregate(events).persist

    val expected = spark.read.json("src/test/resources/time_series_sample_1.json")
    timeSeries.toJSON.collect.map(_.parseJson) should contain theSameElementsAs expected.toJSON.collect.map(_.parseJson)
  }
}
