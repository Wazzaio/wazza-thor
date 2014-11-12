package wazza.thor.jobs

import akka.actor.{ActorRef}

trait WazzaActor {

  protected var supervisor: ActorRef = null

  def inputCollectionType: String
  def outputCollectionType: String
  def getCollectionInput(companyName: String, applicationName: String) =
    s"${companyName}_${inputCollectionType}_${applicationName}"

  def getCollectionOutput(companyName: String, applicationName: String) =
    s"${companyName}_${outputCollectionType}_${applicationName}"

  //Messages
  trait WazzaMessage
  case class InputCollection(companyName: String, applicationName: String) extends WazzaMessage
  case class OutputCollection(companyName: String, applicationName: String) extends WazzaMessage
  case class CoreJobCompleted(companyName: String, applicationName: String, name: String) extends WazzaMessage
}
