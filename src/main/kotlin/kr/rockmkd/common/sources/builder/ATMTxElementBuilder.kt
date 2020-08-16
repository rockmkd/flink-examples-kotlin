package kr.rockmkd.common.sources.builder

import kotlin.random.Random

import kr.rockmkd.common.model.ATMTransaction
import java.io.Serializable
import java.time.LocalDateTime

class ATMTxElementBuilder : ElementBuilder<ATMTransaction>, Serializable {

  private val cards = listOf("1234", "0987", "3333", "4444")
  private var guid = 0

  override fun newElement(): ATMTransaction {
    return ATMTransaction((guid++).toString(), cards.random(), Random.nextInt(1, 1000), LocalDateTime.now()) }
}
