package kr.rockmkd.common.keyselector

import kr.rockmkd.common.model.CardTransaction
import org.apache.flink.api.java.functions.KeySelector

object CardTxKeySelector : KeySelector<CardTransaction, String> {

  override fun getKey(value: CardTransaction): String {
    return value.guid
  }

}
