package wazza.io

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}

class ActorContext(actor: ActorRef) {
  def execute(companyName: String, applicationName: String) = {
    actor ! (companyName, applicationName)
  }
}

