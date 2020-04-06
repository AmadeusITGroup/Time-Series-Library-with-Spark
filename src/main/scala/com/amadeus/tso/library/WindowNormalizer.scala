package com.amadeus.tso.library

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.date_format

/**
  * The trait responsible to transform time windows to match a cyclic schema (daily, weekly, ...).
  * E.g., considering time windows on { 1h, sliding 1h }:
  *
  *   | window.start        | window.end          |
  *   |---------------------+---------------------|
  *   | 2020-02-20 10:00:00 | 2020-02-20 11:00:00 |
  *   | 2020-02-20 11:00:00 | 2020-02-20 12:00:00 |
  *   | 2020-02-20 12:00:00 | 2020-02-20 13:00:00 |
  *   | ...
  *   | 2020-02-21 10:00:00 | 2020-02-21 11:00:00 |
  *   | 2020-02-21 11:00:00 | 2020-02-21 12:00:00 |
  *   | 2020-02-21 12:00:00 | 2020-02-21 13:00:00 |
  *
  *  normalizing to a cycle based on { daily, 1h, sliding 1h } would lead to:
  *
  *   | window.start        | window.end          | normalized.start | normalized.end |
  *   |---------------------+---------------------+------------------+----------------|
  *   | 2020-02-20 10:00:00 | 2020-02-20 11:00:00 |     10:00:00     |     11:00:00   |
  *   | 2020-02-20 11:00:00 | 2020-02-20 12:00:00 |     11:00:00     |     12:00:00   |
  *   | 2020-02-20 12:00:00 | 2020-02-20 13:00:00 |     12:00:00     |     13:00:00   |
  *   | ...
  *   | 2020-02-21 10:00:00 | 2020-02-21 11:00:00 |     10:00:00     |     11:00:00   |
  *   | 2020-02-21 11:00:00 | 2020-02-21 12:00:00 |     11:00:00     |     12:00:00   |
  *   | 2020-02-21 12:00:00 | 2020-02-21 13:00:00 |     12:00:00     |     13:00:00   |
  *
  *  normalizing to a cycle based on { weekly, 1h, 1h } would lead to:
  *
  *   | window.start        | window.end          | normalized.start | normalized.end |
  *   |---------------------+---------------------+------------------+----------------|
  *   | 2020-02-20 10:00:00 | 2020-02-20 11:00:00 |  Thu 10:00:00    |  Thu 11:00:00  |
  *   | 2020-02-20 11:00:00 | 2020-02-20 12:00:00 |  Thu 11:00:00    |  Thu 12:00:00  |
  *   | 2020-02-20 12:00:00 | 2020-02-20 13:00:00 |  Thu 12:00:00    |  Thu 13:00:00  |
  *   | ...
  *   | 2020-02-21 10:00:00 | 2020-02-21 11:00:00 |  Fri 10:00:00    |  Fri 11:00:00  |
  *   | 2020-02-21 11:00:00 | 2020-02-21 12:00:00 |  Fri 11:00:00    |  Fri 12:00:00  |
  *   | 2020-02-21 12:00:00 | 2020-02-21 13:00:00 |  Fri 12:00:00    |  Fri 13:00:00  |
  *
  */
trait WindowNormalizer {
  /**
    * Define the time window normalization logic.
    * @param timeSeries input data, containing a time window column
    * @return the same input data, with one additional columns with the normalized time window (or two for window start and end)
    */
  def normalize(timeSeries: DataFrame) : DataFrame

  /**
    * Declare the list of new columns added in the scope of the normalization.
    * E.g., Seq("start_week_day", "start_time", "end_week_day", "end_time")
    */
  val outputColumnNames : Seq[String]
}

/**
  * Transform time window to a daily-based cycle.
  * E.g., from:
  *
  *   | window.start        | window.end          |
  *   |---------------------+---------------------|
  *   | 2020-02-20 10:00:00 | 2020-02-20 11:00:00 |
  *   | 2020-02-20 11:00:00 | 2020-02-20 12:00:00 |
  *
  *  to:
  *
  *   | window.start        | window.end          | start_time | end_time |
  *   |---------------------+---------------------+------------+----------|
  *   | 2020-02-20 10:00:00 | 2020-02-20 11:00:00 |  10:00:00  | 11:00:00 |
  *   | 2020-02-20 11:00:00 | 2020-02-20 12:00:00 |  11:00:00  | 12:00:00 |
  *
  */
object DailyNormalizer extends WindowNormalizer {
  def normalize(timeSeries: DataFrame) : DataFrame = {
    import timeSeries.sparkSession.implicits._
    timeSeries
      .withColumn("start_time", date_format($"window.start", "HH:mm"))
      .withColumn("end_time", date_format($"window.end", "HH:mm"))
  }

  val outputColumnNames : Seq[String] = Seq("start_time", "end_time")
}

/**
  * Transform time window to a weekly-based cycle.
  * E.g., from:
  *
  *   | window.start        | window.end          |
  *   |---------------------+---------------------|
  *   | 2020-02-20 10:00:00 | 2020-02-20 11:00:00 |
  *   | 2020-02-20 11:00:00 | 2020-02-20 12:00:00 |
  *
  *  to:
  *
  *   | window.start        | window.end          | start_day | start_time | end_day | end_time |
  *   |---------------------+---------------------+-----------+------------+---------+----------|
  *   | 2020-02-20 10:00:00 | 2020-02-20 11:00:00 | Thu       |  10:00:00  | Thu     | 11:00:00 |
  *   | 2020-02-20 11:00:00 | 2020-02-20 12:00:00 | Thu       |  11:00:00  | Thu     | 12:00:00 |
  *
  */
object WeeklyNormalizer extends WindowNormalizer {
  def normalize(timeSeries: DataFrame) : DataFrame = {
    import timeSeries.sparkSession.implicits._
    timeSeries
      .withColumn("start_day", date_format($"window.start", "u"))
      .withColumn("start_time", date_format($"window.start", "HH:mm"))
      .withColumn("end_day", date_format($"window.end", "u"))
      .withColumn("end_time", date_format($"window.end", "HH:mm"))
  }

  val outputColumnNames : Seq[String] = Seq("start_day", "start_time", "end_day", "end_time")
}

/**
  * No transformation. This is useful when the goal is to apply the same analysis criteria on all lines of the time series.
  */
object FlatNormalizer extends WindowNormalizer {
  def normalize(timeSeries: DataFrame) : DataFrame = timeSeries
  val outputColumnNames : Seq[String] = Seq()
}