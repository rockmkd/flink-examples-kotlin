package kr.rockmkd.window

import kr.rockmkd.common.EnvironmentFactory
import kr.rockmkd.common.sources.PaymentsSource
import kr.rockmkd.common.model.UserPayment
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import java.sql.Timestamp
import java.time.LocalDateTime
import kotlin.math.max
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


fun main() {

  val env = EnvironmentFactory.default()

  env.addSource(PaymentsSource())
    .assignTimestampsAndWatermarks(UserPaymentAssignerWithPeriodicWatermarks())
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//    .reduce(PaymentReduceFunction())
    .process(PaymentProcessAllWindowFunction())
    .print()

  env.execute()
}

class PaymentReduceFunction : ReduceFunction<UserPayment> {

  override fun reduce(value1: UserPayment, value2: UserPayment): UserPayment {
    println(value1)
    println(value2)
    return UserPayment(
      value1.userId + value2.userId,
      (value1.cash ?: 0) + (value2.cash ?: 0),
      LocalDateTime.now()
    )
  }

}

class PaymentProcessAllWindowFunction: ProcessAllWindowFunction<UserPayment, Int, TimeWindow>() {

  override fun process(context: Context, elements: MutableIterable<UserPayment>, out: Collector<Int>) {
    println("processing. elements: " + elements.last())
    out.collect(elements.fold(0, {acc, userPayment -> acc + userPayment.cash}))
  }

}

class UserPaymentAssignerWithPeriodicWatermarks : AssignerWithPeriodicWatermarks<UserPayment> {

  private var maxTimeStamp: Long = 0

  override fun extractTimestamp(element: UserPayment?, previousElementTimestamp: Long): Long {
    val currentTimeStamp by lazy {
      element?.let {
        Timestamp.valueOf(it.occurrenceDateTime).time
      } ?: previousElementTimestamp
    }

    maxTimeStamp = max(currentTimeStamp, maxTimeStamp)

    return currentTimeStamp
  }

  override fun getCurrentWatermark(): Watermark? {
    return Watermark(maxTimeStamp)
  }

}


