package kr.rockmkd.common.model

import java.time.LocalDateTime

data class TransactionEvent(
  val guid: String,
  val transactionType: String,
  val tx1EventDateTime: LocalDateTime,
  val tx2EventDateTime: LocalDateTime,
  val joiningDateTime: LocalDateTime
)
