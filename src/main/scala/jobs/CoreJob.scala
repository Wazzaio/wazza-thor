package wazza.thor.jobs

import akka.actor.{ActorRef, Actor, ActorLogging}
import wazza.thor.messages._
import akka.actor.PoisonPill

trait CoreJob extends WazzaActor {
 
  var dependants: List[ActorRef] = Nil
  
  def addDependant(d: ActorRef) = dependants = dependants :+ d

  def kill: Unit

  def onJobSuccess(companyName: String, applicationName: String) = {
    supervisor ! JobCompleted("Total Revenue", new Success)
    dependants.foreach{_ ! CoreJobCompleted("Total Revenue", companyName, applicationName)}
    kill
  }

  def onJobFailure(ex: Exception) = {
    supervisor ! JobCompleted("Total Revenue", new Failure(ex))
    dependants.foreach{_ ! PoisonPill}
    kill
  }
}
