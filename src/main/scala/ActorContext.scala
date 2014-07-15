package wazza.io

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}

class ActorContext(actor: ActorRef) {
  private var inputCollection = null
  private var outputCollection = null

  def getCollections(companyName: String, applicationName: String) = {

  }

  def execute = {
    println(s"input $inputCollection | output $outputCollection")
    println(actor)
    actor ! (inputCollection, outputCollection)
  }
}
