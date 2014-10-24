package wazza.thor.jobs

import akka.actor.{ActorRef}

trait CoreJob {

  var dependants: List[ActorRef] = Nil
  
  def addDependant(d: ActorRef) = dependants = dependants :+ d
}
