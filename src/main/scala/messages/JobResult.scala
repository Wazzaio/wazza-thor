package wazza.thor.messages

import java.util.Date

trait JobResult
case class Success extends JobResult
case class Failure(ex: Exception) extends JobResult
case class JobCompleted(jobName: String, status: JobResult)
case class CoreJobCompleted(
  companyName: String,
  applicationName: String,
  name: String,
  lower: Date,
  upper: Date,
  platforms: List[String]
) extends JobResult

