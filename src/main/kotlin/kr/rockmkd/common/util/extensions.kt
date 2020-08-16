package kr.rockmkd.common.util

import java.time.LocalDateTime
import java.time.ZoneOffset

fun LocalDateTime.toTimestamp() = this.toEpochSecond(ZoneOffset.ofHours(9)) * 1000 + this.nano.toLong() / 1_000_000


