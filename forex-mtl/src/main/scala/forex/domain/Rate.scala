package forex.domain

case class Rate(
    pair: Rate.Pair,
    price: Price,
    timestamp: Timestamp
)

object Rate {
  final case class Pair(
      from: Currency,
      to: Currency
  )

  object Pair {
    val allPairs: List[Pair] = for {
      from <- Currency.values
      to <- Currency.values
      if from != to
    } yield Pair(from, to)
  }
}
