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

import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import scala.concurrent._
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps
import wazza.thor.NotificationMessage
import wazza.thor.NotificationsActor
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json.Json

object AverageRevenuePerSession {
  def props(sc: SparkContext): Props = Props(new AverageRevenuePerSession(sc))
}

class AverageRevenuePerSession(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "avgRevenueSession"

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String],
    paymentSystems: List[Int]
  ): Future[Unit] = {
    val promise = Promise[Unit]

    val revCollName = s"${companyName}_TotalRevenue_${applicationName}"
    val nrSessionsCollName = s"${companyName}_numberSessions_${applicationName}"

    val optRevenue = getResultsByDateRange(revCollName, start, end)
    val optNrSessions = getResultsByDateRange(nrSessionsCollName, start, end)

    val results = (optRevenue, optNrSessions) match {
      case (Some(totalRevenue), Some(nrSessions)) => {
        val avgRevenueSessions = if(nrSessions.result > 0) totalRevenue.result / nrSessions.result else 0
        val platformResults = (totalRevenue.platforms zip nrSessions.platforms).sorted map {p =>
          val aAvgRevenueSessions = if(p._2.res > 0) p._1.res / p._2.res else 0
          val paymentsResults = paymentSystems map {psys =>
            val revPaymentSysOpt = p._1.paymentSystems.get.find(_.system.toInt == psys)
              revPaymentSysOpt match {
              case Some(revPaymentSys) => {
                val resSys = if(p._2.res > 0) revPaymentSys.res / p._2.res else 0.0
                new PaymentSystemResult(psys, resSys)
              }
              case _ => {
                new PaymentSystemResult(psys, 0.0)
              }
            }
            
          }

          new PlatformResults(p._1.platform, aAvgRevenueSessions, Some(paymentsResults))
        }
        new Results(avgRevenueSessions, platformResults, start, end)
      }
      case _ => {
        log.info("One of collections is empty")
        new Results(
          0.0,
          platforms map {new PlatformResults(_, 0.0, Some(paymentSystems map {p => new PaymentSystemResult(p, 0.0)}))},
          start,
          end
        )
      }
    }

    saveResultToDatabase(ThorContext.URI, getCollectionOutput(companyName, applicationName), results)
    promise.success()
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms, paymentSystems) => {
      try {
        log.info(s"core job ended ${sender.toString}")
        updateCompletedDependencies(sender)
        if(dependenciesCompleted) {
          log.info("execute job")
          executeJob(companyName, applicationName, lower, upper, platforms, paymentSystems) map { arpu =>
            log.info("Job completed successful")
            onJobSuccess(companyName, applicationName, self.path.name)
          } recover {
            case ex: Exception => onJobFailure(ex, self.path.name)
          }
        }
      } catch {
        case ex: Exception => {
          log.error(ex.getStackTraceString)
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - Avg Revenue Per Session", ex.getStackTraceString)
          onJobFailure(ex, self.path.name)
        }
      }
    }
    case CoreJobDependency(ref) => {
      log.info(s"Updating core dependencies: ${ref.toString}")
      addDependencies(ref)
    }
    case _ => log.debug("Received invalid message")
  }
}

