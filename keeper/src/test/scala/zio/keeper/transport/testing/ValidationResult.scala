package zio.keeper.transport.testing

import zio.test.TestResult

final case class ValidationResult(values: List[TestResult]) { self =>

  def ++ (that: ValidationResult): ValidationResult =
    ValidationResult(that.values ++ self.values)

  def add(result: TestResult): ValidationResult =
    ValidationResult(result :: values)

  val continue =
    values.forall(_.isSuccess)

  def toTestResult: Option[TestResult] =
    values.reverse.foldLeft[Option[TestResult]](None) {
      case (None, result)   => Some(result)
      case (Some(fst), snd) => Some((fst ==> snd) && fst)
    }

  def transform(f: TestResult => TestResult): ValidationResult =
    ValidationResult(values.map(f))

}

object ValidationResult {

  val empty: ValidationResult = ValidationResult(Nil)

  def of(result: TestResult): ValidationResult =
    ValidationResult(List(result))

}
