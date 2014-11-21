package wazza.thor.messages

import java.util.Date

case class InitJob(companyName: String, applicationName: String, lowerDate: Date, upperDate: Date)

