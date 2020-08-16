package kr.rockmkd.common.sources.builder

import kr.rockmkd.common.model.CardTransaction
import java.io.Serializable
import java.time.LocalDateTime
import kotlin.random.Random

class CardTxElementBuilder : ElementBuilder<CardTransaction>, Serializable {

  private val cards = listOf("1234", "0987", "3333", "4444")
  private var guid = 0

  override fun newElement(): CardTransaction {
    return CardTransaction((guid++).toString(), cards.random(), Random.nextInt(1, 1000), LocalDateTime.now())
  }

}
