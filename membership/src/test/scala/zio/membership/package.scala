package zio

import zio.macros.delegate._
import zio.logging.Logging

package object membership {
  def withNoOpLogging[A] = enrichWith[Logging[A]](NoOpLogging[A])
}
