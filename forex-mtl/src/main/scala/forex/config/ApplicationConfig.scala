package forex.config

import scala.concurrent.duration.FiniteDuration

case class ApplicationConfig(
    http: HttpConfig,
    oneFrame: OneFrameConfig,
    cache: CacheConfig
)

case class HttpConfig(
    host: String,
    port: Int,
    timeout: FiniteDuration
)

case class OneFrameConfig(
    baseUri: String,
    authToken: String,
    timeout: FiniteDuration
)

case class CacheConfig(
    ttl: FiniteDuration,
    softTtl: FiniteDuration,
    maxStaleOnError: FiniteDuration
)
