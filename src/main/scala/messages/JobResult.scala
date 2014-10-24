package wazza.thor.messages

trait JobResult
case class Success extends JobResult
case class Failure extends JobResult
case class JobCompleted(jobName: String, status: JobResult)
