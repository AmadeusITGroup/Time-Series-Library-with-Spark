package com.amadeus.tso.library

import org.apache.spark.sql.{Column, DataFrame}

/**
  * The trait to implement for the analysis of time series and identification of anomalies.
  * The analysis is based on:
  *  - assign a category to each primary key (i.e., monitored entity)
  *  - comparing indicators to the expected values (fixed threshold or ratio) for the assigned category
  *  - identify and filter occurrences that are anomalous against the expected behaviours
  * The analysis requires three input datasets:
  *  - time series to analyze: { window, entity identifiers, indicators }
  *  - entity-to-category mapping: { entity identifiers, category }
  *  - reference model, with the expected behaviour for each category: { category, expected thresholds }
  *
  */
trait AnalyzerTrait {
  /**
    * List the names of columns necessary to identify the entity. These columns must be both in the time series
    *   and in the entity-to-category datasets.
    * E.g., Seq("office", "sign")
    * @return sequence of column names (strings)
    */
  def entityPrimaryKey: Seq[String]

  /**
    * Declare the category to assign to all entities not found in the entity-to-category mappping.
    * E.g., "human".
    * @return default category identifier, as String
    */
  def defaultCategory: String

  /**
    * Choose an implementation of WindowNormalizer to transform time windows and match the reference model.
    * E.g., com.amadeus.tso.library.FlatNormalizer
    * @return an implementation of WindowNormalizer
    */
  def windowNormalizer: WindowNormalizer

  /**
    * Define the condition for a line of the time series to be considered as anomalous, in general as a combination of
    *   indicators from time series and thresholds in the reference model. Keep time series lines in the result if the
    *   condition is verified, discard other lines.
    * E.g., col("attemptCount") > col("allowedAttemptCount") || col("attemptRatio") > col("allowedAttemptRatio")
    * @return a column of Boolean identifying the condition to identify an occurrence as an anomaly
    */
  def isAnomaly: Column

  /**
    * Define the transformation from the internal time series format to the alert format expected on client side.
    * E.g., Seq(
    *   struct(
    *     col("retrieverOffice") as "office",
    *     col("retrieverSign") as "sign"
    *   ) as "retriever",
    *   col("attemptCount") as "retrieves",
    *   lit("ABC") as "alertType"
    * )
    * @return sequence of columns (of various types)
    */
  def translateAnomalySchema: Seq[Column]

  /**
    * The method performing the core processing, from time series to anomalies:
    *  - normalize time windows
    *  - assign categories to entities in the time series
    *  - join with the reference model, based on categories
    *  - filter anomalies comparing actual indicators against the reference model
    *
    * The details of the processing depend on methods definitions in the child/implementation class.
    *
    * @param timeSeries input dataframe: { time window, entity, indicators}
    * @param profilesMap input dataframe: { entity, category }
    * @param model input dataframe: { category, expected thresholds }
    * @return anomalies in the target format, as dataframe
    */
  def detectAnomalies(profilesMap: DataFrame, model: DataFrame)(timeSeries: DataFrame): DataFrame = {
    val modelPrimaryKey = Seq("model") ++ windowNormalizer.outputColumnNames
    timeSeries
      .join(profilesMap, entityPrimaryKey, "leftouter")
      .na
      .fill(defaultCategory, Seq("model"))
      .transform(windowNormalizer.normalize)
      .join(model, modelPrimaryKey, "leftouter")
      .where(isAnomaly)
      .select(translateAnomalySchema:_*)
  }
}
