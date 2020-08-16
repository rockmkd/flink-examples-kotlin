package kr.rockmkd.common.util

import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import kotlin.test.assertEquals

internal class ExtensionsKtTest {

  @Test
  fun 시간값테스트() {
    val dateTime = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123456789)
    assertEquals( 1577901845123, dateTime.toTimestamp())
  }

}
