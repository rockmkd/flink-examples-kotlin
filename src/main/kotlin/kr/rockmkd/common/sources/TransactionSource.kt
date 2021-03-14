package kr.rockmkd.common.sources

import kr.rockmkd.common.sources.builder.ElementBuilder
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import kotlin.random.Random

class TransactionSource<T>(
  private val frequency: Int,
  private val elementBuilder: ElementBuilder<T>
) : RichSourceFunction<T>() {

  private var isRunning = true

  override fun run(ctx: SourceFunction.SourceContext<T>) {
    while (isRunning) {
      val randomDelay = 0L //Random.nextLong(-1000L, 1000L)
      Thread.sleep(1000 / frequency.toLong() + randomDelay)
      ctx.collect(elementBuilder.newElement())
    }
  }

  override fun cancel() {
    isRunning = false
  }

}
