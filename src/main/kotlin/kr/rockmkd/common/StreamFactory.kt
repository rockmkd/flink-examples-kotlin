package kr.rockmkd.common

import kr.rockmkd.common.model.CardTransaction
import kr.rockmkd.common.model.DepositTransaction
import kr.rockmkd.common.sources.TransactionSource
import kr.rockmkd.common.sources.builder.CardTxElementBuilder
import kr.rockmkd.common.sources.builder.DepositTxElementBuilder
import kr.rockmkd.common.util.toTimestamp
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object StreamFactory {

  fun defaultDepositTransaction(env: StreamExecutionEnvironment): SingleOutputStreamOperator<DepositTransaction> =
    env.addSource(
      TransactionSource<DepositTransaction>(1, DepositTxElementBuilder()),
      TypeInformation.of(DepositTransaction::class.java)
    ).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps<DepositTransaction>()
        .withTimestampAssigner { event, _ -> event.dateTime.toTimestamp() }
    )

  fun defaultCardTransaction(env: StreamExecutionEnvironment): SingleOutputStreamOperator<CardTransaction> =
    env.addSource(
      TransactionSource<CardTransaction>(1, CardTxElementBuilder()),
      TypeInformation.of(CardTransaction::class.java)
    ).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps<CardTransaction>()
        .withTimestampAssigner { event, _ -> event.dateTime.toTimestamp() }
    )
}
