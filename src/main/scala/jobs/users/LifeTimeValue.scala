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
import play.api.libs.json._
import org.joda.time.LocalDate

//TODO missing user retention rate
object LifeTimeValue {
  def props(sc: SparkContext): Props = Props(new LifeTimeValue(sc))
}

class LifeTimeValue(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  private val ProfitMargin = 0.70
  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "LifeTimeValue"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: Double,
    end: Date,
    start: Date,
    companyName: String,
    applicationName: String
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val obj = MongoDBObject(
      "lifeTimeValue" -> result,
      "lowerDate" -> start.getTime,
      "upperDate" -> end.getTime
    )
    collection.insert(obj)
    client.close
  }

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date
  ): Future[Unit] = {
    val promise = Promise[Unit]

    def getNumberSecondsBetweenDates(d1: Date, d2: Date): Float = {
      (new LocalDate(d2).toDateTimeAtCurrentTime.getMillis - new LocalDate(d1).toDateTimeAtCurrentTime().getMillis) / 1000
    }

    val dateFields = ("lowerDate", "upperDate")
    val arpuCollName = s"${companyName}_Arpu_${applicationName}"
    val nrSessionsUserCollName = s"${companyName}_numberSessionsPerUser_${applicationName}"
    val uri = MongoClientURI(ThorContext.URI)
    val mongoClient = MongoClient(uri)
    val arpuCollection = mongoClient(uri.database.getOrElse("dev"))(arpuCollName)
    val nrSessionsUserCollection = mongoClient(uri.database.getOrElse("dev"))(nrSessionsUserCollName)

    val query = (dateFields._1 $gte end.getTime $lte start.getTime) ++ (dateFields._2 $gte end.getTime $lte start.getTime)
    val arpuOpt = arpuCollection.findOne(query).getOrElse(None)
    val nrSessionsUserOpt = nrSessionsUserCollection.findOne(query).getOrElse(None)

    (arpuOpt, nrSessionsUserOpt) match {
      case (None,_) | (_,None) | (None,None) => {
        log.error("One of the collections is empty")
        promise.failure(new Exception)
      }
      case (arpu, nrSessionsUser) => {
        val arpuValue = (Json.parse(arpu.toString) \ "arpu").as[Double]
        val nrSessionsuserlst = (Json.parse(nrSessionsUser.toString) \ "nrSessionsPerUser").as[JsArray].value
        val nrUsers = nrSessionsuserlst.size
        val nrSessions = nrSessionsuserlst.foldLeft(0.0)((acc, el) => {
          acc + (el \ "nrSessions").as[Int]
        })
        log.info("number sessions " + nrSessions + " | nr users " + nrUsers)
        val avgSessionsUser = if(nrUsers > 0) nrSessions / nrUsers else 0
        //TODO - missing user retention rate
        val ltv = arpuValue * avgSessionsUser * ProfitMargin
        saveResultToDatabase(
          ThorContext.URI,
          getCollectionOutput(companyName, applicationName),
          ltv,
          start,
          end,
          companyName,
          applicationName
        )
        promise.success()
      } 
    }

    mongoClient.close
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
          log.info("Job completed successful")
          onJobSuccess(companyName, applicationName, "LTV")
        } recover {
          case ex: Exception => onJobFailure(ex, "LTV")
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

