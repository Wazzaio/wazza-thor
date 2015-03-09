package wazza.thor.jobs

import akka.actor.{ActorRef, Actor, ActorLogging}
import wazza.thor.messages._
import akka.actor.PoisonPill
import java.util.Date

trait CoreJob extends WazzaActor {
  self: Actor =>

  protected var jobCompleted = false

  var dependants: List[ActorRef] = Nil
  protected var childJobsCompleted = List[String]()
  def addDependant(d: ActorRef) = dependants = dependants :+ d
  def kill: Unit

  def onJobSuccess(
    companyName: String,
    applicationName: String,
    jobType: String,
    lower: Date,
    upper: Date,
    platforms: List[String]
  ) = {
    dependants.foreach{_ ! CoreJobCompleted(companyName, applicationName, jobType, lower, upper, platforms)}
    jobCompleted = true
  }

  def onJobFailure(ex: Exception, jobType: String) = {
    supervisor ! JobCompleted(jobType, new WZFailure(ex))
    dependants.foreach{_ ! PoisonPill}
    kill
  }
}
