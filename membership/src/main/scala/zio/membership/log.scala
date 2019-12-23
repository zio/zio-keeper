package zio.membership

import zio._
import zio.logging.Logging

object log extends Logging.Service[Logging[String], String] {

  def trace(message: => String)                    = ZIO.accessM(_.logging.trace(message))
  def debug(message: => String)                    = ZIO.accessM(_.logging.debug(message))
  def info(message: => String)                     = ZIO.accessM(_.logging.info(message))
  def warning(message: => String)                  = ZIO.accessM(_.logging.warning(message))
  def error(message: => String)                    = ZIO.accessM(_.logging.error(message))
  def error(message: => String, cause: Cause[Any]) = ZIO.accessM(_.logging.error(message, cause))

}
