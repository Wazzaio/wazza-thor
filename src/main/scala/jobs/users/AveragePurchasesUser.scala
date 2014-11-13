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
import scala.collection.mutable.Map

object AveragePurchasesUser {
  def props(sc: SparkContext): Props = Props(new AveragePurchasesUser(sc))
}

class AveragePurchasesUser(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "avgPurchasesUser"

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
      "avgPurchasesUser" -> result,
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
    val uri = MongoClientURI(ThorContext.URI)
    val mongoClient = MongoClient(uri)
    val payingUsersCollection = mongoClient(uri.database.getOrElse("dev"))(payingUsersCollName)
    
    val query = (dateFields._1 $gte end.getTime $lte start.getTime) ++ (dateFields._2 $gte end.getTime $lte start.getTime)
    val payingUsers = payingUsersCollection.findOne(query).getOrElse(None)

    payingUsers match {
      case None => {
        log.error("cannot find elements")
        promise.failure(new Exception)
      }
      case p => {
        val UserKey = "user"
        val PurchasesKey = "purchases"
        val mapResult = (Json.parse(p.toString) \ "payingUsers").as[JsArray].value.foldLeft(Map.empty[String, Double]){(map, el) =>
          val userPurchases = (el \ "purchases").as[JsArray].value.size
          val storedPurchases = map getOrElse(PurchasesKey, 0.0)
          val storedUsers = map getOrElse(UserKey, 0.0)
          map += (UserKey -> (storedUsers + 1))
          map += (PurchasesKey -> (storedPurchases + userPurchases))
        }

        val result = if(mapResult(UserKey) > 0) mapResult(PurchasesKey) / mapResult(UserKey) else 0
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
          onJobSuccess(companyName, applicationName, "Average Revenue Per Session")
        } recover {
          case ex: Exception => onJobFailure(ex, "Average Revenue Per Session")
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


