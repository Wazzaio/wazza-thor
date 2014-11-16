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

object PurchasesPerSession {
  def props(sc: SparkContext): Props = Props(new PurchasesPerSession(sc))
}

class PurchasesPerSession(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "PurchasesPerSession"

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
      "avgPurchasesSession" -> result,
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

    val dateFields = ("lowerDate", "upperDate")
    val payingUsersCollName = s"${companyName}_payingUsers_${applicationName}"
    val nrSessionsCollName = s"${companyName}_numberSessions_${applicationName}"
    val uri = MongoClientURI(ThorContext.URI)
    val mongoClient = MongoClient(uri)
    val payingUsersCollection = mongoClient(uri.database.getOrElse("dev"))(payingUsersCollName)
    val nrSessionsCollection = mongoClient(uri.database.getOrElse("dev"))(nrSessionsCollName)
    
    val query = (dateFields._1 $gte end.getTime $lte start.getTime) ++ (dateFields._2 $gte end.getTime $lte start.getTime)
    val nrSessions = nrSessionsCollection.findOne(query).getOrElse(None)
    val payingUsers = payingUsersCollection.findOne(query).getOrElse(None)

    (nrSessions, payingUsers) match {
      case (None, None) => {
        log.error("cannot find elements")
        promise.failure(new Exception)
      }
      case (sessions, users) => {
        val nrSessions = (Json.parse(sessions.toString) \ "totalSessions").as[Int]
        val nrPurchases = (Json.parse(users.toString) \ "payingUsers").as[JsArray].value.foldLeft(0.0)((sum, obj) => {
          sum + (obj \ "purchases").as[JsArray].value.size
        })

        val result = if(nrSessions > 0) nrPurchases / nrSessions else 0
        saveResultToDatabase(
          ThorContext.URI,
          getCollectionOutput(companyName, applicationName),
          result,
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
          onJobSuccess(companyName, applicationName, "Average Purchases Per Session")
        } recover {
          case ex: Exception => onJobFailure(ex, "Average Purchases Per Session")
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

