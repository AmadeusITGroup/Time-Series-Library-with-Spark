package com.amadeus.tso.library

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/**
  * The trait to implement for the aggregation of time series from raw events.
  * Raw events need to contain the information about:
  *  - timestamp of the event
  *  - identifiers of the entity to monitor
  *  - relevant data to aggregate
  * Define the members according to the functional details of the time series.
  */
trait AggregatorTrait {
  /**
    * Declare or compute the column containing the timestamp of input events.
    * E.g., col("timestamp")
    * E.g., from_unixtime(col("Epoch")/1000)
    * @return column of TimestampType (ie java.sql.Timestamp values or equivalent)
    */
  def timestampColumn: Column

  /**
    * Declare the time series window in CalendarInterval format.
    * E.g., "1 hour", "20 minutes", ...
    * Refer to the org.apache.spark.sql.functions.window function for more details.
    * @return string in CalendarInterval format
    */
  def timeSeriesWindowDuration: String

  /**
    * Declare the time series slide duration in CalendarInterval format.
    * E.g., "1 hour", "20 minutes", ...
    * Refer to the org.apache.spark.sql.functions.window function for more details.
    * @return string in CalendarInterval format
    */
  def timeSeriesSlideDuration: String

  /**
    * Declare or compute the columns to manipulate (either as primary key or as input for indicators) from
    * the input events.
    * E.g., Seq(col("office"), col("sign"), col("action"), col("container_id"))
    * @return sequence of columns (of various type)
    */
  def featureSelection: Seq[Column]

  /**
    * Compute the filtering condition as a column of Boolean (when true, keep the event, else discard it).
    * Use lit("true") to include all input events in the processing.
    * E.g., col("org") =!= lit("1A0")
    * @return column of Boolean
    */
  def filteringCriteria: Column

  /**
    * Declare or compute the columns identifying the primary key for the time series (i.e., the entity to monitor and
    * for which to compute indicators and detect anomalies).
    * E.g., Seq(col("office"), col("sign"))
    * @return sequence of columns
    */
  def primaryKey: Seq[Column]

  /**
    * Compute the output indicators based on data in the input events. The output indicators are considered as aggregated
    * by time window and primary key (e.g., total amount over a given hour for one agent).
    * E.g., Seq(
    *     countDistinct("container_id").as("distinct_containers"),
    *     count(lit(1)).as("attemptCount")
    * )
    * @return sequence of column (of various type)
    */
  def aggregation: Seq[Column]

  /**
    * The method performing the core processing, from raw events to time series:
    * - aggregate events by time window
    * - filter events (optionally)
    * - aggregate by primary key
    * - compute indicators
    * The computation details depend on the definitions provided in the child/implementation classes.
    *
    * @param input raw events, containing a timestamp column
    * @return time series
    */
  def aggregate(input: DataFrame): DataFrame = {
    input
      .select(Seq(timestampColumn.cast(TimestampType)) ++: featureSelection: _*)
      .where(filteringCriteria)
      .groupBy(
        Seq(window(timestampColumn, timeSeriesWindowDuration, timeSeriesSlideDuration)) ++ primaryKey: _*
      )
      .agg(aggregation.head, aggregation.tail: _*)
      .na
      .fill(0)
  }
}
