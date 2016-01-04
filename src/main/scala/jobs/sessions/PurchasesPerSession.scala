/*
 * wazza-thor
 * https://github.com/Wazzaio/wazza-thor
 * Copyright (C) 2013-2015  Duarte Barbosa, João Vazão Vasques
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
import wazza.thor.NotificationMessage
import wazza.thor.NotificationsActor
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._

object PurchasesPerSession {
  def props(sc: SparkContext): Props = Props(new PurchasesPerSession(sc))

  def mapPayingUsersRDD(rdd: RDD[Tuple2[Object, BSONObject]], platforms: List[String], paymentSystems: List[Int]) = {
    rdd map {element =>
      val totalPurchases = Json.parse(element._2.get("purchases").toString).as[JsArray].value.size
      val purchasesPerPlatform = platforms map {platform =>
        val p = Json.parse(element._2.get("purchasesPerPlatform").toString).as[JsArray]
        val nrPurchases = p.value.find(el => (el \ "platform").as[String] == platform) match {
          case Some(platformPurchasesJson) => {
            // Get total purchases on this platform as well as by payment system
            val platformPurchases = (platformPurchasesJson \ "purchases").as[JsArray].value.toList
            val totalPurchases = platformPurchases.size
            val purchasesPerPaymentSystem = paymentSystems map {system =>
              new PurchasesPerPaymentSystem(
                system,
                platformPurchases.count(pp => (pp \ "paymentSystem").as[Double].toInt == system)
              )
            }
            (totalPurchases, purchasesPerPaymentSystem)
          }
          case _ => (0, paymentSystems map {new PurchasesPerPaymentSystem(_, 0)})
        }
        new PurchasePerPlatform(platform, nrPurchases._1, nrPurchases._2)
      }
      new PurchasePerUser(element._2.get("userId").toString, totalPurchases, purchasesPerPlatform)
    }
  }

  def reducePayingUsers(rdd: RDD[PurchasePerUser], platforms: List[String], paymentSystems: List[Int]): PurchasePerUser = {
    rdd.reduce{(res, current) =>
      val total = res.totalPurchases + current.totalPurchases
      val purchasesPerPlatforms = platforms map {p =>
        def purchaseCalculator(pps: List[PurchasePerPlatform]): Double = {
          pps.find(_.platform == p).get.purchases
        }
        val updatedPurchases = purchaseCalculator(current.platforms) + purchaseCalculator(res.platforms)
        val updatedPurchasesPerPayments = paymentSystems map {system =>
          def calculatePaymentsPerSystem(el: PurchasePerUser): Int = {
            el.platforms.find(_.platform == p).get.paymentSystemsPurchases.count(
              _.paymentSystem == system
            )
          }
          //Get current platform element; check if it has the current payment and update the result
          val currentPlatformPayments = calculatePaymentsPerSystem(current)
          val accumPlatformPayments = calculatePaymentsPerSystem(res)
          new PurchasesPerPaymentSystem(system, currentPlatformPayments + accumPlatformPayments)
          }
        val result = new PurchasePerPlatform(p, updatedPurchases, updatedPurchasesPerPayments)
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

/** Purchases case classes **/
sealed case class PurchasesPerPaymentSystem(paymentSystem: Int, purchases: Double)
sealed case class PurchasePerPlatform(
  platform: String,
  purchases: Double,
  paymentSystemsPurchases: List[PurchasesPerPaymentSystem]
)
sealed case class PurchasePerUser(userId: String, totalPurchases: Double, platforms: List[PurchasePerPlatform])
object PurchasePerUser {
  def apply: PurchasePerUser = new PurchasePerUser(null, 0, List[PurchasePerPlatform]())
}

/** Sessions case classes **/
sealed case class SessionsPerPlatform(platform: String, sessions: Double)
sealed case class NrSessions(total: Double, platforms: List[SessionsPerPlatform])
object NrSessions {
  def apply: NrSessions = new NrSessions(0, List[SessionsPerPlatform]())
}

/** Results **/
sealed case class PaymentSystemsResults(paymentSystem: Int, value: Double)
sealed case class PurchasesPerSessionPerPlatform(platform: String, value: Double, paymentSystems: List[PaymentSystemsResults])
sealed case class PurchasesPerSessionResult(total: Double, platforms: List[PurchasesPerSessionPerPlatform])

class PurchasesPerSession(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "payingUsers"
  def outputCollectionType: String = "PurchasesPerSession"

  private implicit def PurchasesPerSessionPerPlatformToBson(p: PurchasesPerSessionPerPlatform): MongoDBObject = {
    MongoDBObject(
      "platform" -> p.platform,
      "res" -> p.value,
      "paymentSystems" -> (p.paymentSystems map {e =>
        MongoDBObject("system" -> e.paymentSystem, "res" -> e.value)
      })
    )
  }

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: PurchasesPerSessionResult,
    start: Date,
    end: Date,
    companyName: String,
    applicationName: String,
    platforms: List[String],
    paymentSystems: List[Int]
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val platformResults = if(result.platforms.isEmpty) {
      platforms.map {p => MongoDBObject(
        "platform" -> p,
        "res" -> 0.0,
        "paymentSystems" -> (paymentSystems map {e => MongoDBObject("system" -> e, "res" -> 0.0)}))
      }
    } else {
      result.platforms.map{PurchasesPerSessionPerPlatformToBson(_)}
    }
    val obj = MongoDBObject(
      "result" -> result.total,
      "platforms" -> platformResults,
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
    platforms: List[String],
    paymentSystems: List[Int]
  ): PurchasePerUser = {
    val inputUri = s"${ThorContext.URI}.${inputCollection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val payingUsersRDD = filterRDDByDateFields(
      ("lowerDate", "upperDate"),
      sc.newAPIHadoopRDD(
        jobConfig,
        classOf[com.mongodb.hadoop.MongoInputFormat],
        classOf[Object],
        classOf[BSONObject]
      ),
      start,
      end,
      sc
    )

    if(payingUsersRDD.count > 0) {
      PurchasesPerSession.reducePayingUsers(
        PurchasesPerSession.mapPayingUsersRDD(payingUsersRDD, platforms, paymentSystems), platforms, paymentSystems
      )
    } else {
      log.info("Count is zero")
      PurchasePerUser.apply
    }
  }

  private def getNumberSessions(collection: String, start: Date, end: Date, platforms: List[String]): NrSessions = {
    val inputUri = s"${ThorContext.URI}.${collection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val sessionsRDD = filterRDDByDateFields(
      ("lowerDate", "upperDate"),
      sc.newAPIHadoopRDD(
        jobConfig,
        classOf[com.mongodb.hadoop.MongoInputFormat],
        classOf[Object],
        classOf[BSONObject]
      ),
      start,
      end,
      sc
    )

    if(sessionsRDD.count > 0) {
      PurchasesPerSession.reduceSessions(
        PurchasesPerSession.mapSessionsRDD(sessionsRDD, platforms), platforms
      )
    } else {
      log.info("empty session collection")
      NrSessions.apply
    }
  }

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String],
    paymentSystems: List[Int]
  ): Future[Unit] = {
    val promise = Promise[Unit]

    val purchases = getNumberPurchases(
      getCollectionInput(companyName, applicationName),
      companyName,
      applicationName,
      start,
      end,
      platforms,
      paymentSystems
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
      val paymentSystemsResults = paymentSystems map {system =>
        current._1.paymentSystemsPurchases.find(_.paymentSystem == system) match {
          case Some(paymentSystemInfo) => {
            val res = if(current._2.sessions > 0) paymentSystemInfo.purchases / current._2.sessions else 0.0
            new PaymentSystemsResults(system, res)
          }
          case None => {new PaymentSystemsResults(system, 0.0)}
        }
      }
      res :+ new PurchasesPerSessionPerPlatform(current._1.platform, result, paymentSystemsResults)
    }}

    saveResultToDatabase(
      ThorContext.URI,
      getCollectionOutput(companyName, applicationName),
      new PurchasesPerSessionResult(totalResult, resultPlatforms),
      start,
      end,
      companyName,
      applicationName,
      platforms,
      paymentSystems
    )
    promise.success()
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms, paymentSystems) => {
      try {
        log.info(s"core job ended ${sender.toString}")
        updateCompletedDependencies(sender)
        if(dependenciesCompleted) {
          log.info("execute job")
          executeJob(companyName, applicationName, lower, upper, platforms, paymentSystems) map { arpu =>
            log.info("Job completed successful")
            onJobSuccess(companyName, applicationName, self.path.name)
          } recover {
            case ex: Exception => onJobFailure(ex, self.path.name)
          }
        }
      } catch {
        case ex: Exception => {
          log.error(ex.getStackTraceString)
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - PURCHASES PER SESSION", ex.getStackTraceString)
          onJobFailure(ex, self.path.name)
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

