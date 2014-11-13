package wazza.thor.jobs

import akka.actor.{ActorRef, Actor, ActorLogging}
import wazza.thor.messages._
import akka.actor.PoisonPill
import java.util.Date

trait CoreJob extends WazzaActor {
  self: Actor =>

  var dependants: List[ActorRef] = Nil
  def addDependant(d: ActorRef) = dependants = dependants :+ d
  def kill: Unit

  def onJobSuccess(companyName: String, applicationName: String, jobType: String, lower: Date, upper: Date) = {
    supervisor ! JobCompleted(jobType, new Success)
    dependants.foreach{_ ! CoreJobCompleted(companyName, applicationName, jobType, lower, upper)}
    kill
  }

  def onJobFailure(ex: Exception, jobType: String) = {
    supervisor ! JobCompleted(jobType, new Failure(ex))
    dependants.foreach{_ ! PoisonPill}
    kill
  }
}
