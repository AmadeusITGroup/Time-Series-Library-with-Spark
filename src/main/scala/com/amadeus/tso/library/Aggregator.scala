package com.amadeus.tso.library

import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Implementation of the Aggregator trait, receiving implementation details through a typesafe.Config parameter.
  * The filteringCriteria and aggregation definitions are not part of this class and need manual implementation in the
  * child classes.
  * @param config typesafe.Config object providing implementation details through set fields:
  *  - TimestampColumn: declare the timestamp column in the input events (no transformation)
  *  - WindowDuration: declare the window duration in CalendarInterval format
  *  - SlideDuration: declare the slide duration in CalendarInterval format
  *  - Features: declare the columns to select for the processing, as a comma-separated string
  *  - PrimaryKey: declare the columns to select as primary key, as a comma-separated string
  */
abstract class Aggregator(config: Config) extends AggregatorTrait {
  override def timestampColumn: Column = col(config.getString("TimestampColumn"))
  override def timeSeriesSlideDuration: String = config.getString("SlideDuration")
  override def timeSeriesWindowDuration: String = config.getString("WindowDuration")

  override def featureSelection: Seq[Column] = config.getString("Features")
    .split(",")
    .map(c => col(c))

  override def primaryKey: Seq[Column] = config.getString("PrimaryKey")
    .split(",")
    .map(c => col(c))
}
