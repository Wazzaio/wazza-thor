package wazza.io

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}

class ActorContext(actor: ActorRef) {
  def execute(companyName: String, applicationName: String) = {
    println(actor)
    actor ! (companyName, applicationName)
  }
}

