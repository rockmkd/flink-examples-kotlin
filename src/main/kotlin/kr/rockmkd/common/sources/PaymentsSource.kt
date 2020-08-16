package kr.rockmkd.common.sources

import kr.rockmkd.common.model.UserPayment
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.time.LocalDateTime
import kotlin.random.Random

class PaymentsSource : RichSourceFunction<UserPayment>() {

  private var running = true

  override fun run(ctx: SourceFunction.SourceContext<UserPayment>) {
    while (running) {
      Thread.sleep(Random.nextLong(100L, 2000L))
      val userPayment = UserPayment(
        "user" + Random.nextInt(1, 6),
        Random.nextInt(1000, 10000),
        LocalDateTime.now()//.minusNanos(Random.nextLong(100_000L, 2000_000L))
      )
      ctx.collect(userPayment)
      println(userPayment)
    }
  }

  override fun cancel() {
    running = false
  }

}
