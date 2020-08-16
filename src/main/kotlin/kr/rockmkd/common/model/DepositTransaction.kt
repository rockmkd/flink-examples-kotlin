package kr.rockmkd.common.model

import java.time.LocalDateTime

data class DepositTransaction(
  val guid: String,
  val accountNo: String,
  val price: Int,
  val dateTime: LocalDateTime
)
