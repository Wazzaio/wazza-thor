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
    platforms: List[String],
    paymentSystems: List[Int]
  ) = {
    dependants.foreach{_ ! CoreJobCompleted(companyName, applicationName, jobType, lower, upper, platforms, paymentSystems)}
    jobCompleted = true
  }

  def onJobFailure(ex: Exception, jobType: String) = {
    supervisor ! JobCompleted(jobType, new WZFailure(ex))
    dependants.foreach{_ ! PoisonPill}
    kill
  }
}
