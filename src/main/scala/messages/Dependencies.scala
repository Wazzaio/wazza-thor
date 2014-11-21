package wazza.thor.messages

import akka.actor.{ActorRef}

trait Dependencies
case class CoreJobDependency(refs: List[ActorRef]) extends Dependencies

