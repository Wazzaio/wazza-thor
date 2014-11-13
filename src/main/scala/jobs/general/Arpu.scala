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

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    arpu: Double,
    lowerDate: Date,
    upperDate: Date,
    companyName: String,
    applicationName: String
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val obj = MongoDBObject(
      "arpu" -> arpu,
      "lowerDate" -> lowerDate.getTime,
      "upperDate" -> upperDate.getTime
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
    val revCollName = s"${companyName}_TotalRevenue_${applicationName}"
    val activeCollName = s"${companyName}_activeUsers_${applicationName}"
    val uri = MongoClientURI(ThorContext.URI)
    val mongoClient = MongoClient(uri)
    val revCollection = mongoClient(uri.database.getOrElse("dev"))(revCollName)
    val activeCollection = mongoClient(uri.database.getOrElse("dev"))(activeCollName)
    
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
        val arpu = if(a > 0) r / a else 0
        
        saveResultToDatabase(
          ThorContext.URI,
          getCollectionOutput(companyName, applicationName),
          arpu,
          start,
          end,
          companyName,
          applicationName
        )
        promise.success()
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
          log.info("Job completed successful")
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

