package wazza.io

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import java.util.Date

class ActorContext(actor: ActorRef) {
  def execute(
    companyName: String,
    applicationName: String,
    lowerDate: Date,
    upperDate: Date
  ) = {
    actor ! (companyName, applicationName, lowerDate, upperDate)
  }
}

