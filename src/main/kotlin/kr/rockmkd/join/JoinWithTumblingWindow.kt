package kr.rockmkd.join

import kr.rockmkd.common.EnvironmentFactory
import kr.rockmkd.common.StreamFactory
import kr.rockmkd.common.keyselector.CardTxKeySelector
import kr.rockmkd.common.keyselector.DepositTxKeySelector
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object JoinWithTumblingWindow {

  @JvmStatic
  fun main(args: Array<String>) {
    val env = EnvironmentFactory.default(3)

    val depositStream = StreamFactory.defaultDepositTransaction(env)
    val cardTxStream = StreamFactory.defaultCardTransaction(env)

    depositStream.join(cardTxStream)
      .where(DepositTxKeySelector)
      .equalTo(CardTxKeySelector)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(TransactionJoinFunction)
      .print()

    env.execute()
  }

}

