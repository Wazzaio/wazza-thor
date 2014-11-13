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
import ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json.Json

object Arpu {
  def props(sc: SparkContext): Props = Props(new Arpu(sc))
}

class Arpu(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "Arpu"

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date
  ): Future[Double] = {
    val promise = Promise[Double]

    val dateFields = ("lowerDate", "upperDate")
    val revCollName = s"${companyName}_TotalRevenue_${applicationName}"
    val activeCollName = s"${companyName}_activeUsers_${applicationName}"
    val mongoClient = MongoClient(MongoClientURI(ThorContext.URI))
    val revCollection = mongoClient("dev")(revCollName)
    val activeCollection = mongoClient("dev")(activeCollName)
    
    val query = (dateFields._1 $gte end.getTime $lte start.getTime) ++ (dateFields._2 $gte end.getTime $lte start.getTime)
    val activeUsers = activeCollection.findOne(query).getOrElse(None)
    val totalRevenue = revCollection.findOne(query).getOrElse(None)

    (activeUsers, totalRevenue) match {
      case (None, None) => {
        log.info("cannot find elements")
        promise.failure(new Exception)
      }
      case (active, revenue) => {
        val a = (Json.parse(active.toString) \ "activeUsers").as[Int]
        val r = (Json.parse(revenue.toString) \ "totalRevenue").as[Double]
        log.info(a.toString)
        log.info(r.toString)
        promise.success(if(a > 0) r / a else 0)
      }
    }

    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper) => {
      log.info(s"core job ended ${sender.toString}")
      updateCompletedDependencies(sender)
      if(dependenciesCompleted) {
        log.info("execute job")
        executeJob(companyName, applicationName, upper, lower) map { arpu =>
          onJobSuccess[Double](companyName, applicationName, "Arpu", arpu)
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

