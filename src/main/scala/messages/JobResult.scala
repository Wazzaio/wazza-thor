package wazza.thor.messages

import java.util.Date

trait JobResult
case class WZSuccess extends JobResult
case class WZFailure(ex: Exception) extends JobResult
case class JobCompleted(jobName: String, status: JobResult)
case class CoreJobCompleted(
  companyName: String,
  applicationName: String,
  name: String,
  lower: Date,
  upper: Date,
  platforms: List[String],
  paymentSystems: List[Int]
) extends JobResult

