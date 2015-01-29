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
    platforms: List[String]
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
          new PlatformResults(p._1.platform, aAvgRevenueSessions)
        }
        new Results(avgRevenueSessions, platformResults, start, end)
      }
      case _ => {
        log.info("One of collections is empty")
        new Results(0.0, platforms map {new PlatformResults(_, 0.0)}, start, end)
      }
    }

    saveResultToDatabase(ThorContext.URI, getCollectionOutput(companyName, applicationName), results)
    promise.success()
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms) => {
      try {
        log.info(s"core job ended ${sender.toString}")
        updateCompletedDependencies(sender)
        if(dependenciesCompleted) {
          log.info("execute job")
          executeJob(companyName, applicationName, lower, upper, platforms) map { arpu =>
            log.info("Job completed successful")
            onJobSuccess(companyName, applicationName, "Average Revenue Per Session")
          } recover {
            case ex: Exception => onJobFailure(ex, "Average Revenue Per Session")
          }
        }
      } catch {
        case ex: Exception => {
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - Avg Revenue Per Session", ex.getStackTraceString)
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

