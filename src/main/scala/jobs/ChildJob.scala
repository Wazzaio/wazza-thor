/*
 * wazza-thor
 * https://github.com/Wazzaio/wazza-thor
 * Copyright (C) 2013-2015  Duarte Barbosa, João Vazão Vasques
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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

