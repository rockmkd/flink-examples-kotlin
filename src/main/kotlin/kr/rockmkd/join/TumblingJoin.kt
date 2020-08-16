package kr.rockmkd.join

import kr.rockmkd.common.EnvironmentFactory
import kr.rockmkd.common.model.CardTransaction
import kr.rockmkd.common.model.DepositTransaction
import kr.rockmkd.common.model.TransactionEvent
import kr.rockmkd.common.sources.TransactionSource
import kr.rockmkd.common.sources.builder.CardTxElementBuilder
import kr.rockmkd.common.sources.builder.DepositTxElementBuilder
import kr.rockmkd.common.util.toTimestamp
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import java.time.Duration
import java.time.LocalDateTime

object TumblingJoin {

  @JvmStatic
  fun main(args: Array<String>) {
    val env = EnvironmentFactory.default(3)

    val depositStream =
      env.addSource(
        TransactionSource<DepositTransaction>(1, DepositTxElementBuilder()),
        TypeInformation.of(DepositTransaction::class.java)
      ).assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<DepositTransaction>(Duration.ofSeconds(1))
          .withTimestampAssigner { event, _ -> event.dateTime.toTimestamp() }
      )

    val cardTxStream =
      env.addSource(
        TransactionSource<CardTransaction>(1, CardTxElementBuilder()),
        TypeInformation.of(CardTransaction::class.java)
      ).assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness<CardTransaction>(Duration.ofSeconds(1))
          .withTimestampAssigner { event, _ -> event.dateTime.toTimestamp() }
      )

    depositStream.join(cardTxStream)
      .where(DepositTxKeySelect())
      .equalTo(CardTxKeySelect())
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(TransactionJoinFunction)
      .print()

    env.execute()
  }

}

class DepositTxKeySelect : KeySelector<DepositTransaction, String> {

  override fun getKey(value: DepositTransaction): String {
    return value.guid
  }

}

class CardTxKeySelect : KeySelector<CardTransaction, String> {

  override fun getKey(value: CardTransaction): String {
    return value.guid
  }

}

object TransactionJoinFunction : JoinFunction<DepositTransaction, CardTransaction, TransactionEvent> {

  override fun join(first: DepositTransaction, second: CardTransaction): TransactionEvent {
    return TransactionEvent(
      first.guid + "_" + second.guid,
      "card",
      first.dateTime,
      second.dateTime,
      LocalDateTime.now()
    )
  }

}
