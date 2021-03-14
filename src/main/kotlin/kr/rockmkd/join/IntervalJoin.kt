package kr.rockmkd.join

import kr.rockmkd.common.EnvironmentFactory
import kr.rockmkd.common.StreamFactory
import kr.rockmkd.common.keyselector.CardTxKeySelector
import kr.rockmkd.common.keyselector.DepositTxKeySelector
import kr.rockmkd.common.model.CardTransaction
import kr.rockmkd.common.model.DepositTransaction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoin {

    @JvmStatic
    fun main(args: Array<String>) {
        val env = EnvironmentFactory.default(3)

        val depositStream = StreamFactory.defaultDepositTransaction(env)
        val cardTxStream = StreamFactory.defaultCardTransaction(env)

        depositStream
            .filter{ it.price > 100}
            .map{
                it.copy(guid=it.guid, accountNo = it.accountNo, price = it.price+100, dateTime = it.dateTime).apply{
                    println(this)
                }
            }
            .keyBy{ deposit -> deposit.guid }
            .intervalJoin(cardTxStream.map{it.copy(guid=it.guid, cardNo = it.cardNo).apply { println(it) }}.keyBy{ card -> card.guid })
            .between(Time.seconds(-1), Time.seconds(1))
            .process(object: ProcessJoinFunction<DepositTransaction, CardTransaction, String>() {
                override fun processElement(
                    left: DepositTransaction,
                    right: CardTransaction,
                    ctx: Context,
                    out: Collector<String>
                ) {
                    println("====")
                    println( left)
                    println(right)
                    out.collect("out")
                }

            })
            .print()
//        depositStream.join(cardTxStream)
//            .where(DepositTxKeySelector)
//            .equalTo(CardTxKeySelector)
//            .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//            .apply(TransactionJoinFunction)
//            .print()

        env.execute()
    }

}