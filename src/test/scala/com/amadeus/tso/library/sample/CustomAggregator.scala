package com.amadeus.tso.library.sample

import com.amadeus.tso.library.AggregatorTrait
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

class CustomAggregator extends AggregatorTrait {
  override def timestampColumn: Column = col("timestamp")
  override def timeSeriesWindowDuration: String = "1 hour"
  override def timeSeriesSlideDuration: String = "1 hour"

  override def featureSelection: Seq[Column] = Seq(
    col("office"),
    col("userId"),
    col("action"),
    col("container")
  )

  override def filteringCriteria: Column = lit(true)

  override def primaryKey: Seq[Column] = Seq(
    col("office"),
    col("userId")
  )

  override def aggregation: Seq[Column] = Seq(
    count(lit(1)) as "attemptCount",
    countDistinct("container") as "uniqueContainerCount",
    collect_set("container") as "containers",
    collect_set("action") as "actions"
  )
}
