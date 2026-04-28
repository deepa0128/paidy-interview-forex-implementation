package forex.domain

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CurrencySpec extends AnyWordSpec with Matchers with EitherValues {

  "Currency.fromString" should {

    "parse every supported currency code" in {
      val codes = List("AUD", "CAD", "CHF", "EUR", "GBP", "NZD", "JPY", "SGD", "USD")
      codes.foreach { code =>
        Currency.fromString(code).value shouldBe a[Currency]
      }
    }

    "be case-insensitive" in {
      Currency.fromString("usd").value shouldBe Currency.USD
      Currency.fromString("Eur").value shouldBe Currency.EUR
      Currency.fromString("jPy").value shouldBe Currency.JPY
    }

    "return Left for unsupported codes" in {
      Currency.fromString("XYZ").left.value should include("XYZ")
      Currency.fromString("BTC").left.value should include("BTC")
    }

    "return Left for an empty string" in {
      Currency.fromString("").isLeft shouldBe true
    }

    "return Left for whitespace-only input" in {
      Currency.fromString("   ").isLeft shouldBe true
    }
  }

  "Currency.values" should {

    "contain exactly 9 currencies" in {
      Currency.values should have size 9
    }

    "contain no duplicates" in {
      Currency.values.distinct.size shouldBe Currency.values.size
    }
  }

  "Rate.Pair.allPairs" should {

    "contain exactly 72 pairs (9 × 8)" in {
      Rate.Pair.allPairs should have size 72
    }

    "never include a pair where from == to" in {
      Rate.Pair.allPairs.foreach { pair =>
        pair.from should not be pair.to
      }
    }

    "include every ordered combination of supported currencies" in {
      val expected = for {
        from <- Currency.values
        to <- Currency.values
        if from != to
      } yield Rate.Pair(from, to)

      Rate.Pair.allPairs should contain theSameElementsAs expected
    }
  }

  "Currency.show" should {

    "produce the ISO 4217 code string" in {
      Currency.show.show(Currency.USD) shouldBe "USD"
      Currency.show.show(Currency.JPY) shouldBe "JPY"
    }

    "round-trip through fromString for all currencies" in {
      Currency.values.foreach { currency =>
        Currency.fromString(Currency.show.show(currency)).value shouldBe currency
      }
    }
  }
}
