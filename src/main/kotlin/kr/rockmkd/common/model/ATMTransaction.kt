package kr.rockmkd.common.model

import java.time.LocalDateTime

data class ATMTransaction(
  val guid: String,
  val atmNo: String,
  val price: Int,
  val dateTime: LocalDateTime
)
