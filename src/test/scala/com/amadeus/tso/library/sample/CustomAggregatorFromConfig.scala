package com.amadeus.tso.library.sample

import com.amadeus.tso.library.Aggregator
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{collect_set, count, countDistinct, lit}

class CustomAggregatorFromConfig(config: Config) extends Aggregator(config) {
  override def aggregation: Seq[Column] = Seq(
    count(lit(1)) as "attemptCount",
    countDistinct("container") as "uniqueContainerCount",
    collect_set("container") as "containers",
    collect_set("action") as "actions"
  )

  override def filteringCriteria: Column = lit(true)
}
