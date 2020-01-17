package zio.membership

import zio._
import zio.logging.Logging

object NoOpLogging {

  def apply[A]: Logging[A] = new Logging[A] {

    val logging = new Logging.Service[Any, A] {
      def trace(message: => A)                    = ZIO.unit
      def debug(message: => A)                    = ZIO.unit
      def info(message: => A)                     = ZIO.unit
      def warning(message: => A)                  = ZIO.unit
      def error(message: => A)                    = ZIO.unit
      def error(message: => A, cause: Cause[Any]) = ZIO.unit
    }
  }

}
