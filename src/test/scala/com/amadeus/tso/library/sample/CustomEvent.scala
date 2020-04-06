package com.amadeus.tso.library.sample

case class CustomEvent
(
  timestamp: java.sql.Timestamp,
  office: String,
  userId: String,
  action: String,
  container: String
)
