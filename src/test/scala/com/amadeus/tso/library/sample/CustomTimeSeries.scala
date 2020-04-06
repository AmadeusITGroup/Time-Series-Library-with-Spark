package com.amadeus.tso.library.sample

case class Window
(
  start: java.sql.Timestamp,
  end: java.sql.Timestamp
)

case class CustomTimeSeries
(
  window: Window,
  office: String,
  userId: String,
  attemptCount: Long,
  uniqueContainerCount: Long,
  containers: Seq[String],
  actions: Seq[String]
)
