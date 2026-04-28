package forex.services.rates

object errors {

  sealed trait Error
  object Error {
    final case object OneFrameQuotaExceeded extends Error
    final case class OneFrameUnreachable(cause: Throwable) extends Error
    final case class OneFrameLookupFailed(msg: String) extends Error
  }

}
