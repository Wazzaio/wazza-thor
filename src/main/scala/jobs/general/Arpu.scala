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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import scala.collection.immutable.StringOps
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json.Json

object Arpu {
  def props(sc: SparkContext, ltvJob: ActorRef): Props = Props(new Arpu(sc, ltvJob))
}

class Arpu(sc: SparkContext, ltvJob: ActorRef) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "Arpu"

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): Future[Unit] = {
    val promise = Promise[Unit]

    val revCollName = s"${companyName}_TotalRevenue_${applicationName}"
    val activeCollName = s"${companyName}_activeUsers_${applicationName}"

    val optRevenue = getResultsByDateRange(revCollName, start, end)
    val optActive = getResultsByDateRange(activeCollName, start, end)

    val results = (optRevenue, optActive) match {
      case (Some(totalRevenue), Some(activeUsers)) => {
        val arpu = if(activeUsers.result > 0) totalRevenue.result / activeUsers.result else 0
        val platformResults = (totalRevenue.platforms zip activeUsers.platforms).sorted map {p =>
          val pArpu = if(p._2.res > 0) p._1.res / p._2.res else 0
          new PlatformResults(p._1.platform, pArpu)
        }
        new Results(arpu, platformResults, start, end)      
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
      log.info(s"core job ended ${sender.toString}")
      updateCompletedDependencies(sender)
      if(dependenciesCompleted) {
        log.info("execute job")
        executeJob(companyName, applicationName, lower, upper, platforms) map { arpu =>
          log.info("Job completed successful")
          ltvJob ! CoreJobCompleted(companyName, applicationName, "Arpu", lower, upper, platforms)
          onJobSuccess(companyName, applicationName, "Arpu")
        } recover {
          case ex: Exception => onJobFailure(ex, "Arpu")
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

