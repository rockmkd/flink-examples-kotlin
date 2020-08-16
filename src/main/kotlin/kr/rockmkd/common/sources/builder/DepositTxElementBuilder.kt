package kr.rockmkd.common.sources.builder

import kr.rockmkd.common.model.DepositTransaction
import java.io.Serializable
import java.time.LocalDateTime

class DepositTxElementBuilder : ElementBuilder<DepositTransaction>, Serializable {

  private val accounts = listOf("A", "B", "C", "D")
  private var guid = 0

  override fun newElement(): DepositTransaction {
    return DepositTransaction((guid++).toString(), accounts.random(), 1000, LocalDateTime.now())
  }

}
