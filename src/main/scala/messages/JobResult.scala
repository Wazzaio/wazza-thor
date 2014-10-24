package wazza.thor.messages

trait JobResult
case class Success extends JobResult
case class Failure(ex: Exception) extends JobResult
case class JobCompleted(jobName: String, status: JobResult)
