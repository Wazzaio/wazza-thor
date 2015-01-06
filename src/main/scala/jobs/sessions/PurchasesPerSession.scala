package wazza.thor.jobs

import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
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

object PurchasesPerSession {
  def props(sc: SparkContext): Props = Props(new PurchasesPerSession(sc))

  def mapPayingUsersRDD(rdd: RDD[Tuple2[Object, BSONObject]], platforms: List[String]) = {
    rdd map {element =>
      val totalPurchases = Json.parse(element._2.get("purchases").toString).as[JsArray].value.size
      val purchasesPerPlatform = platforms map {platform =>
        val p = Json.parse(element._2.get("purchasesPerPlatform").toString).as[JsArray]
        val nrPurchases = p.value.find(el => (el \ "platform").as[String] == platform) match {
          case Some(platformPurchases) => (platformPurchases \ "purchases").as[JsArray].value.size
          case _ => 0
        }
        new PurchasePerPlatform(platform, nrPurchases)
      }
      new PurchasePerUser(element._2.get("userId").toString, totalPurchases, purchasesPerPlatform)
    }
  }

  def reducePayingUsers(rdd: RDD[PurchasePerUser], platforms: List[String]): PurchasePerUser = {
    rdd.reduce{(res, current) =>
      val total = res.totalPurchases + current.totalPurchases
      val purchasesPerPlatforms = platforms map {p =>
        def purchaseCalculator(pps: List[PurchasePerPlatform]) = {
          pps.find(_.platform == p).get.purchases
        }
        val updatedPurchases = purchaseCalculator(current.platforms) + purchaseCalculator(res.platforms)
        val result = new PurchasePerPlatform(p, updatedPurchases)
        result
      }
      new PurchasePerUser(null, total, purchasesPerPlatforms.toList)
    }
  }

  def mapSessionsRDD(rdd: RDD[Tuple2[Object, BSONObject]], platforms: List[String]) = {
    rdd map {element =>
      val totalSessions = Json.parse(element._2.get("result").toString).as[Double]
      val sessionsPerPlatform = platforms map {platform =>
        val p = Json.parse(element._2.get("platforms").toString).as[JsArray]
        val nrSessions = p.value.find(el => (el \ "platform").as[String] == platform) match {
          case Some(s) => (s \ "res").as[Double]
          case _ => 0
        }
        new SessionsPerPlatform(platform, nrSessions)
      }
      new NrSessions(totalSessions, sessionsPerPlatform)
    }
  }

  def reduceSessions(rdd: RDD[NrSessions], platforms: List[String]): NrSessions = {
    rdd reduce{(res, current) => {
      val totalSessions = res.total + current.total
      val sessionsPerPlatforms = platforms map {p =>
        def sessionCalculator(pps: List[SessionsPerPlatform]) = {
          pps.find(_.platform == p).get.sessions
        }
        val updatedPurchases = sessionCalculator(current.platforms) + sessionCalculator(res.platforms)
        new SessionsPerPlatform(p, updatedPurchases)
      }
      new NrSessions(totalSessions, sessionsPerPlatforms)
    }}
  }
}

sealed case class PurchasePerPlatform(platform: String, purchases: Double)
sealed case class PurchasePerUser(userId: String, totalPurchases: Double, platforms: List[PurchasePerPlatform])
object PurchasePerUser {
  def apply: PurchasePerUser = new PurchasePerUser(null, 0, List[PurchasePerPlatform]())
}

sealed case class SessionsPerPlatform(platform: String, sessions: Double)
sealed case class NrSessions(total: Double, platforms: List[SessionsPerPlatform])
object NrSessions {
  def apply: NrSessions = new NrSessions(0, List[SessionsPerPlatform]())
}

sealed case class PurchasesPerSessionPerPlatform(platform: String, value: Double)
sealed case class PurchasesPerSessionResult(total: Double, platforms: List[PurchasesPerSessionPerPlatform])

class PurchasesPerSession(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "payingUsers"
  def outputCollectionType: String = "PurchasesPerSession"

  private implicit def PurchasesPerSessionPerPlatformToBson(p: PurchasesPerSessionPerPlatform): MongoDBObject = {
    MongoDBObject("platform" -> p.platform, "value" -> p.value)
  }

  private implicit def PurchasesPerSessionResultToBson(p: PurchasesPerSessionResult): MongoDBObject = {
    MongoDBObject("total" -> p.total, "platforms" -> p.platforms.map(PurchasesPerSessionPerPlatformToBson(_)))
  }

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: PurchasesPerSessionResult,
    end: Date,
    start: Date,
    companyName: String,
    applicationName: String
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val obj = MongoDBObject(
      "avgPurchasesSession" -> PurchasesPerSessionResultToBson(result),
      "lowerDate" -> start,
      "upperDate" -> end
    )
    collection.insert(obj)
    client.close
  }

  private def getNumberPurchases(
    inputCollection: String,
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): PurchasePerUser = {
    val inputUri = s"${ThorContext.URI}.${inputCollection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val payingUsersRDD = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )/**.filter((t: Tuple2[Object, BSONObject]) => {
      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toDouble.toLong) } catch { case _: Throwable => None }
      }
      parseFloat(t._2.get("lowerDate").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(start) * end.compareTo(startDate) >= 0
        }
        case _ => false
      }
    })**/

    if(payingUsersRDD.count > 0) {
      PurchasesPerSession.reducePayingUsers(
        PurchasesPerSession.mapPayingUsersRDD(payingUsersRDD, platforms), platforms
      )
    } else {
      log.error("Count is zero")
      PurchasePerUser.apply
    }
  }

  private def getNumberSessions(collection: String, start: Date, end: Date, platforms: List[String]): NrSessions = {
    val inputUri = s"${ThorContext.URI}.${collection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val sessionsRDD = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )//TODO TIME FILTER

    if(sessionsRDD.count > 0) {
      PurchasesPerSession.reduceSessions(
        PurchasesPerSession.mapSessionsRDD(sessionsRDD, platforms), platforms
      )
    } else {
      log.error("empty session collection")
      NrSessions.apply
    }
  }

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): Future[Unit] = {
    val promise = Promise[Unit]

    val purchases = getNumberPurchases(
      getCollectionInput(companyName, applicationName),
      companyName,
      applicationName,
      start,
      end,
      platforms
    )
    val nrSessions = getNumberSessions(s"${companyName}_numberSessions_${applicationName}", start, end, platforms)

    val totalResult = if(nrSessions.total > 0) purchases.totalPurchases / nrSessions.total else 0

    val zippedPlatforms = {
      val purchasesPerPlatform = purchases.platforms.sortWith{(a,b) => {
        (a.platform compareToIgnoreCase b.platform) < 0
      }}
      val sessionsPerPlatform = nrSessions.platforms.sortWith{(a,b) => {
        (a.platform compareToIgnoreCase b.platform) < 0
      }}
      purchasesPerPlatform zip sessionsPerPlatform
    }

    val resultPlatforms = zippedPlatforms.foldLeft(List[PurchasesPerSessionPerPlatform]()){(res, current) => {
      val result = if(current._2.sessions > 0) current._1.purchases / current._2.sessions else 0
      res :+ new PurchasesPerSessionPerPlatform(current._1.platform, result)
    }}

    saveResultToDatabase(
      ThorContext.URI,
      getCollectionOutput(companyName, applicationName),
      new PurchasesPerSessionResult(totalResult, resultPlatforms),
      end,
      start,
      companyName, applicationName
    )
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms) => {
      log.info(s"core job ended ${sender.toString}")
      updateCompletedDependencies(sender)
      if(dependenciesCompleted) {
        log.info("execute job")
        executeJob(companyName, applicationName, upper, lower, platforms) map { arpu =>
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

