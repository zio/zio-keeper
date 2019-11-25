package zio.membership

// TODO: define error hierarchy
sealed trait Error extends Exception

sealed trait SendError    extends Error
sealed trait ReceiveError extends Error

final case class DeserializationError(msg: String) extends ReceiveError
