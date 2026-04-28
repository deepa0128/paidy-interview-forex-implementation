package forex.services.rates.cache

import cats.effect.IO
import forex.domain.{ Currency, Price, Rate, Timestamp }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.OffsetDateTime
import scala.concurrent.duration._

class RatesCacheSpec extends AnyWordSpec with Matchers {

  val usdEur  = Rate.Pair(Currency.USD, Currency.EUR)
  val gbpJpy  = Rate.Pair(Currency.GBP, Currency.JPY)
  val softTtl = 4.minutes
  val hardTtl = 5.minutes

  def makeRate(pair: Rate.Pair, price: BigDecimal = BigDecimal("1.23")): Rate =
    Rate(pair, Price(price), Timestamp.now)

  def newCache(): InMemoryRatesCache[IO] =
    InMemoryRatesCache.create[IO].unsafeRunSync()

  "InMemoryRatesCache.get" should {

    "return None for a pair that has never been stored" in {
      val cache  = newCache()
      val result = cache.get(usdEur).unsafeRunSync()

      result shouldBe None
    }
  }

  "InMemoryRatesCache.putBatch" should {

    "make a stored pair retrievable via get" in {
      val cache = newCache()
      val rate  = makeRate(usdEur)
      val now   = OffsetDateTime.now()

      cache.putBatch(List(rate), now).unsafeRunSync()

      val stored = cache.get(usdEur).unsafeRunSync()
      stored.map(_.rate) shouldBe Some(rate)
    }

    "store multiple pairs in one call" in {
      val cache  = newCache()
      val rateUE = makeRate(usdEur)
      val rateGJ = makeRate(gbpJpy)
      val now    = OffsetDateTime.now()

      cache.putBatch(List(rateUE, rateGJ), now).unsafeRunSync()

      val storedUE = cache.get(usdEur).unsafeRunSync()
      val storedGJ = cache.get(gbpJpy).unsafeRunSync()

      storedUE.map(_.rate) shouldBe Some(rateUE)
      storedGJ.map(_.rate) shouldBe Some(rateGJ)
    }

    "overwrite an older entry with a fresher one" in {
      val cache    = newCache()
      val original = makeRate(usdEur, BigDecimal("0.90"))
      val updated  = makeRate(usdEur, BigDecimal("0.95"))
      val t1       = OffsetDateTime.now().minusMinutes(3)
      val t2       = OffsetDateTime.now()

      cache.putBatch(List(original), t1).unsafeRunSync()
      cache.putBatch(List(updated), t2).unsafeRunSync()

      val stored = cache.get(usdEur).unsafeRunSync()
      stored.map(_.rate.price) shouldBe Some(Price(BigDecimal("0.95")))
    }
  }

  "InMemoryRatesCache.allKeys" should {

    "return an empty set for a new cache" in {
      val cache = newCache()
      cache.allKeys.unsafeRunSync() shouldBe empty
    }

    "return every pair that has been stored" in {
      val cache = newCache()
      val now   = OffsetDateTime.now()

      cache.putBatch(List(makeRate(usdEur), makeRate(gbpJpy)), now).unsafeRunSync()

      val keys = cache.allKeys.unsafeRunSync()
      keys should contain allOf (usdEur, gbpJpy)
    }
  }

  "CacheEntry freshness" should {

    "be fresh when age is below softTtl" in {
      val now   = OffsetDateTime.now()
      val entry = CacheEntry(makeRate(usdEur), fetchedAt = now.minusSeconds(10))

      entry.isFresh(now, softTtl) shouldBe true
      entry.needsRevalidation(now, softTtl, hardTtl) shouldBe false
      entry.isExpired(now, hardTtl) shouldBe false
    }

    "need revalidation when age is between softTtl and hardTtl" in {
      val now   = OffsetDateTime.now()
      val entry = CacheEntry(makeRate(usdEur), fetchedAt = now.minusSeconds(250))

      entry.isFresh(now, softTtl) shouldBe false
      entry.needsRevalidation(now, softTtl, hardTtl) shouldBe true
      entry.isExpired(now, hardTtl) shouldBe false
    }

    "be expired when age exceeds hardTtl" in {
      val now   = OffsetDateTime.now()
      val entry = CacheEntry(makeRate(usdEur), fetchedAt = now.minusSeconds(310))

      entry.isFresh(now, softTtl) shouldBe false
      entry.needsRevalidation(now, softTtl, hardTtl) shouldBe false
      entry.isExpired(now, hardTtl) shouldBe true
    }

    "be fresh when just inserted (age zero)" in {
      val now   = OffsetDateTime.now()
      val entry = CacheEntry(makeRate(usdEur), fetchedAt = now)

      entry.isFresh(now, softTtl) shouldBe true
    }

    "be expired at exactly the hardTtl boundary" in {
      val now   = OffsetDateTime.now()
      val entry = CacheEntry(makeRate(usdEur), fetchedAt = now.minusMinutes(5))

      entry.isExpired(now, hardTtl) shouldBe true
    }
  }
}
