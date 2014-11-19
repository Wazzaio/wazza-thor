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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorContext}
import scala.collection.immutable.StringOps
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import org.joda.time.LocalDate

private object InternalSparkFunctions extends Serializable {

  var companyName = ""
  var applicationName = ""

  def calculateTimeFirstPurchase(
    info: Tuple2[String, Tuple2[Option[Int], JsValue]]
  ): Tuple2[String, Double] = {
    val pInfo = (Json.parse(info._2._2.toString).as[JsValue])
    val purchaseSession = getSessionByPurchase(
      ThorContext.URI,
      companyName,
      applicationName,
      ((pInfo \ "purchaseId").as[String])
    ).get

    val timePreviousSessions = getTotalTimeFromPreviousSessions(
      ThorContext.URI,
      companyName,
      applicationName,
      purchaseSession
    )

    val timeToPurchaseInSession = calculateTimeToPurchaseWithinSession(purchaseSession, pInfo)
    (info._1, timePreviousSessions + timeToPurchaseInSession)
  }

  def getNumberSecondsBetweenDates(d1: Date, d2: Date): Float = {
    (new LocalDate(d2).toDateTimeAtCurrentTime.getMillis - new LocalDate(d1).toDateTimeAtCurrentTime().getMillis) / 1000
  }

  def getSessionByPurchase(
    uriStr: String,
    companyName: String,
    applicationName: String,
    purchaseId: String
  ): Option[JsValue] = {
    def getSessionId(): String = {
      val collectionName = s"${companyName}_purchases_${applicationName}"
      val uri  = MongoClientURI(uriStr)
      val client = MongoClient(uri)
      val collection = client.getDB(uri.database.get)(collectionName)
      val query = MongoDBObject("id" -> purchaseId)
      val projection = MongoDBObject("sessionId" -> 1)
      collection.findOne(query, projection) match {
        case None => {
          client.close
          null //TODO launch error
        }
        case Some(info) => {
          client.close
          (Json.parse(info.toString) \ "sessionId").as[String]
        }
      }
    }

    val collectionName = s"${companyName}_mobileSessions_${applicationName}"
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val sessionId = getSessionId
    val query = MongoDBObject("id" -> getSessionId)
    collection.findOne(query) match {
      case None => {
        client.close
        None
      }
      case Some(r) => {
        client.close
        Some(Json.parse(r.toString))
      }
    }
  }

  def getTotalTimeFromPreviousSessions(
    uriStr: String,
    companyName: String,
    applicationName: String,
    session: JsValue
  ): Double = {
    val collectionName = s"${companyName}_mobileSession_${applicationName}"
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val sessionTime = (session \ "startTime").as[Float]
    val query = "startTime" $lte sessionTime
    val projection = MongoDBObject("sessionLength" -> 1)
    val result = collection.find(query, projection).toList.foldLeft(0.0){(total,el) =>
      total + (Json.parse(el.toString) \ "sessionLength").as[Double]
    }
    client.close
    result
  }

  def calculateTimeToPurchaseWithinSession(
    session: JsValue,
    purchaseInfo: JsValue
  ): Double = {
    val sessionStartInstant = new Date((session \ "startTime").as[Float].toLong)
    val purchaseInstant = new Date((purchaseInfo \ "time").as[Float].toLong)
    getNumberSecondsBetweenDates(purchaseInstant, sessionStartInstant)
  }
}

object AvgTimeFirstPurchase {
  def props(sc: SparkContext): Props = Props(new AvgTimeFirstPurchase(sc))
}

class AvgTimeFirstPurchase(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  private lazy val Buyers = "Buyers" 

  def inputCollectionType: String = "payingUsers"
  def outputCollectionType: String = "AvgTimeFirstPurchase"

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

  private def getAllBuyers(companyName: String, applicationName: String) = {
    val collectionName = s"${companyName}_${Buyers}_${applicationName}"
    val inputUri = s"${ThorContext.URI}.${collectionName}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")
    sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ) map { _._2.get("userId").toString}
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

    val collection = getCollectionInput(companyName, applicationName)
    val inputUri = s"${ThorContext.URI}.${collection}"
    log.info("INPUT URI " + inputUri)
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val payingUsersRDD = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )/**.filter(element => {
      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toLong) } catch { case _: Throwable => None }
      }

       parseFloat(element._2.get("lowerDate").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(start) * end.compareTo(startDate) >= 0
        }
        case _ => false
      }
    })**/

    if(payingUsersRDD.count > 0) {
      val payingUsers = payingUsersRDD map {_._2.get("userId").toString}
      val allPayingUsers = getAllBuyers(companyName, applicationName)
      val usersIds = payingUsers.subtract(allPayingUsers).map {(_, 1)}
      val purchaseInfo = payingUsersRDD.map {purchases => {
        val pList = Json.parse(purchases._2.get("purchases").toString).as[JsArray].value
        (purchases._2.get("userId").toString, pList.head)
      }}

      val firstTimePayingUsers = usersIds.rightOuterJoin(purchaseInfo)
      log.info(firstTimePayingUsers.collect.toList.toString)
      InternalSparkFunctions.companyName = companyName
      InternalSparkFunctions.applicationName = applicationName
      val r = firstTimePayingUsers.map(InternalSparkFunctions.calculateTimeFirstPurchase)
      log.info("RESULT")
      log.info(r.collect.toList.toString)
      0
    } else {

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

