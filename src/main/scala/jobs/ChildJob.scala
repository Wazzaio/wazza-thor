package wazza.thor.jobs

import akka.actor.{ActorRef, Actor, ActorLogging}
import wazza.thor.messages._
import akka.actor.PoisonPill

trait ChildJob extends WazzaActor {
  self: Actor =>

  var dependencies: List[ActorRef] = Nil
  var completedDependencies: List[ActorRef] = Nil

  def addDependencies(dependencies: List[ActorRef]) = dependencies.foreach{addDependency(_)}
  def addDependency(d: ActorRef) = dependencies = dependencies :+ d
  
  def updateCompletedDependencies(d: ActorRef) = completedDependencies = completedDependencies :+ d
  def dependenciesCompleted = {
    dependencies.foldLeft(dependencies.size == completedDependencies.size)(_ && completedDependencies.contains(_))
  }
  def kill: Unit
  
  def onJobSuccess(companyName: String, applicationName: String, jobType: String) {
    dependencies.foreach{_ ! JobCompleted(jobType, new WZSuccess)}
    kill
  }

  def onJobFailure(ex: Exception, jobType: String) = {
    dependencies.foreach{_ ! JobCompleted(jobType, new WZFailure(ex))}
    kill
  }
}

