package com.amadeus.tso.library.sample

import com.amadeus.tso.library.{AnalyzerTrait, FlatNormalizer, WindowNormalizer}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

class CustomAnalyzer extends AnalyzerTrait {
  override def entityPrimaryKey: Seq[String] = Seq("office", "userId")

  override def defaultCategory: String = "DEFAULT"

  override def windowNormalizer: WindowNormalizer = FlatNormalizer

  override def isAnomaly: Column = col("uniqueContainerCount") > col("allowedContainerCount")

  override def translateAnomalySchema: Seq[Column] = Seq(
    col("window.start").as("access_timestamp"),
    concat(col("office"), lit("-"), col("userId")).as("retriever"),
    col("model"),
    col("uniqueContainerCount").as("containers"),
    col("allowedContainerCount").as("allowedContainers"),
    col("actions")
  )
}
