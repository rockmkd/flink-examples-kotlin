package kr.rockmkd.common.keyselector

import kr.rockmkd.common.model.DepositTransaction
import org.apache.flink.api.java.functions.KeySelector

object DepositTxKeySelector : KeySelector<DepositTransaction, String> {

  override fun getKey(value: DepositTransaction): String {
    return value.guid
  }

}
