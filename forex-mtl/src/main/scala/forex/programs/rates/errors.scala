package forex.programs.rates

import forex.services.rates.errors.{ Error => RatesServiceError }

object errors {

  sealed trait Error extends Exception

  object Error {

    final case class InvalidCurrency(input: String) extends Error {
      override def getMessage: String = s"Unsupported currency: $input"
    }

    final case class UpstreamUnavailable(msg: String) extends Error {
      override def getMessage: String = msg
    }

    final case class SystemError(msg: String) extends Error {
      override def getMessage: String = msg
    }
  }

  def toProgramError(error: RatesServiceError): Error = error match {
    case RatesServiceError.OneFrameQuotaExceeded      => Error.UpstreamUnavailable("One-Frame API quota exceeded for today")
    case RatesServiceError.OneFrameUnreachable(cause) => Error.UpstreamUnavailable(s"One-Frame is unreachable: ${cause.getMessage}")
    case RatesServiceError.OneFrameLookupFailed(msg)  => Error.UpstreamUnavailable(s"One-Frame error: $msg")
  }
}
