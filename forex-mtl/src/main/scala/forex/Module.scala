package forex

import cats.effect.{ Concurrent, Resource, Timer }
import cats.syntax.semigroupk._
import forex.config.ApplicationConfig
import forex.http.{ HealthRoutes, RateLimiter, RequestId }
import forex.http.rates.RatesHttpRoutes
import forex.programs.RatesProgram
import forex.services.{ RatesService, RatesServiceFactory }
import org.http4s.{ HttpApp, HttpRoutes }
import org.http4s.client.Client
import org.http4s.implicits._
import org.http4s.server.middleware.{ AutoSlash, Timeout }

class Module[F[_]: Concurrent: Timer](
    config: ApplicationConfig,
    val ratesService: RatesService[F],
    rateLimiter: HttpRoutes[F] => HttpRoutes[F]
) {
  type PartialMiddleware = HttpRoutes[F] => HttpRoutes[F]
  type TotalMiddleware   = HttpApp[F] => HttpApp[F]

  private val ratesProgram: RatesProgram[F]    = RatesProgram[F](ratesService)
  private val ratesHttpRoutes: HttpRoutes[F]   = new RatesHttpRoutes[F](ratesProgram).routes
  private val healthRoutes: HttpRoutes[F]      = new HealthRoutes[F](ratesService.isReady).routes
  private val allRoutes: HttpRoutes[F]         = ratesHttpRoutes <+> healthRoutes
  private val routesMiddleware: PartialMiddleware = AutoSlash(_)
  private val appMiddleware: TotalMiddleware   = Timeout(config.http.timeout)(_)

  val httpApp: HttpApp[F] =
    appMiddleware(
      rateLimiter(
        RequestId.middleware(routesMiddleware(allRoutes))
      ).orNotFound
    )
}

object Module {

  def resource[F[_]: Concurrent: Timer](
      config: ApplicationConfig,
      httpClient: Client[F]
  ): Resource[F, Module[F]] =
    for {
      ratesService <- RatesServiceFactory.live[F](httpClient, config)
      rateLimiter  <- Resource.eval(RateLimiter.middleware[F](config.rateLimiter.maxRequestsPerMinute))
    } yield new Module[F](config, ratesService, rateLimiter)
}
