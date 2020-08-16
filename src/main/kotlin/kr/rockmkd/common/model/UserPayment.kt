package kr.rockmkd.common.model

import java.time.LocalDateTime

data class UserPayment(
  val userId: String,
  val cash: Int,
  val occurrenceDateTime: LocalDateTime
)
