package wazza.thor.messages

import java.util.Date

case class InitJob(
  companyName: String,
  applicationName: String,
  platforms: List[String],
  lowerDate: Date,
  upperDate: Date
)

