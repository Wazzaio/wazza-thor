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
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import org.joda.time.LocalDate

object AvgTimeBetweenPurchases {
  def props(sc: SparkContext): Props = Props(new AvgTimeBetweenPurchases(sc))
}

class AvgTimeBetweenPurchases(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "AvgTimeBetweenPurchases"

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
      "avgTimeBetweenPurchases" -> result,
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
    val payingUsersCollName = s"${companyName}_payingUsers_${applicationName}"
    val uri = MongoClientURI(ThorContext.URI)
    val mongoClient = MongoClient(uri)
    val payingUsersCollection = mongoClient(uri.database.getOrElse("dev"))(payingUsersCollName)

    val query = (dateFields._1 $gte end.getTime $lte start.getTime) ++ (dateFields._2 $gte end.getTime $lte start.getTime)

    var numberPurchases = 0
    var accTime = 0.0
    payingUsersCollection.find(query) foreach {element =>
      val purchases = (Json.parse(element.toString) \ "purchases").as[JsArray].value.zipWithIndex
      for(purchaseInfo <- purchases) {
        val index = purchaseInfo._2
        if(purchases.size == 1) {
          numberPurchases += 1
        } else {
          if((index+1) < purchases.size ) {
            val currentPurchaseDate = new Date((purchaseInfo._1 \ "time").as[Float].longValue)
            val nextPurchaseDate = new Date((purchases(index+1)._1 \ "time").as[Float].longValue)
            accTime += getNumberSecondsBetweenDates(currentPurchaseDate, nextPurchaseDate)
          }
        }
      }
    }

    if(numberPurchases > 0 && accTime > 0) {
      val result =  if(numberPurchases == 0) 0 else accTime / numberPurchases
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
    } else {
      log.error("empty collection")
      promise.failure(new Exception)
    }

    mongoClient.close
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms) => {
      log.info(s"core job ended ${sender.toString}")
      updateCompletedDependencies(sender)
      if(dependenciesCompleted) {
        log.info("execute job")
        executeJob(companyName, applicationName, upper, lower) map { arpu =>
          log.info("Job completed successful")
          onJobSuccess(companyName, applicationName, "Average Time Between Purchases, platforms")
        } recover {
          case ex: Exception => onJobFailure(ex, "Average Time Between Purchases")
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

