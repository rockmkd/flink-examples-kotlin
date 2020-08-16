package kr.rockmkd.common.model

import java.time.LocalDateTime

data class CardTransaction(
  val guid: String,
  val cardNo: String,
  val price: Int,
  val dateTime: LocalDateTime
)

