package kr.rockmkd.join

import kr.rockmkd.common.model.CardTransaction
import kr.rockmkd.common.model.DepositTransaction
import kr.rockmkd.common.model.TransactionEvent
import org.apache.flink.api.common.functions.JoinFunction
import java.time.LocalDateTime

object TransactionJoinFunction :
  JoinFunction<DepositTransaction, CardTransaction, TransactionEvent> {

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
