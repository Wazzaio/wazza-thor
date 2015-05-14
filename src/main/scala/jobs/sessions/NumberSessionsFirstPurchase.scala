package wazza.thor.jobs

import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import scala.concurrent._
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorContext}
import scala.collection.immutable.StringOps
import wazza.thor.NotificationMessage
import wazza.thor.NotificationsActor
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import org.joda.time.LocalDate

object NumberSessionsFirstPurchases {
  def props(sc: SparkContext): Props = Props(new NumberSessionsFirstPurchases(sc))


  def getNumberSessionsFirstPurchasePerUser(
    sc: SparkContext, 
    companyName: String, 
    applicationName: String, 
    userIds: List[String], 
    platforms: List[String],
    paymentSystems: List[Int]
  ): RDD[Tuple2[Double, List[Tuple3[String, List[Tuple3[Int, Option[Double], Int]], Int]]]] = {

    val collection = s"${companyName}_mUsers_${applicationName}"
    val inputUri = s"${ThorContext.URI}.${collection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val rdd = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )
    val firstTimeBuyersInfo = rdd.filter(u => userIds.contains(u._2.get("userId").toString))  
    if(firstTimeBuyersInfo.count > 0) {
      firstTimeBuyersInfo.map{userInfo =>
        //Get Purchase Date
        val purchases = Json.parse(userInfo._2.get("purchases").toString).as[JsArray].value.toList
        val purchaseDate = purchases.map(p => {
          new Date((p \ "time").as[Double].toLong)
        }).head

        val sessions = Json.parse(userInfo._2.get("sessions").toString).as[JsArray].value.toList
        val sessionsToPurchase = sessions.count(s => {
          val sessionDate = new Date((s \ "startTime").as[Double].toLong)
          sessionDate.before(purchaseDate) || sessionDate.equals(purchaseDate)
        }).toDouble

        //Calculate number of sessions to 1st purchase per platform & per payment system
        val platformData = platforms map {p =>
          val platformPurchases = purchases.filter(pp => (pp \ "platform").as[String] == p)
          val paymentsData = paymentSystems map {system =>
            platformPurchases.find(ps => (ps \ "paymentSystem").as[Double].toInt == system) match {
              case Some(purchase) => {
                val purchaseDate =  new Date((purchase \ "time").as[Double].toLong)
                val sessionsToPurchase = sessions.count(s => {
                  val sessionDate = new Date((s \ "startTime").as[Double].toLong)
                  val dateValidation = sessionDate.before(purchaseDate) || sessionDate.equals(purchaseDate)
                  val platformValidation = (s \ "platform").as[String] == p
                  dateValidation && platformValidation
                })
                (system, Some(sessionsToPurchase.toDouble), 1)
              }
              case None => (system, None, 0)
            }
          }
          (p, paymentsData, 1)
        }
        (sessionsToPurchase, platformData)
      }
    }
    else {
      sc.emptyRDD
    }
  }

  def getFirstTimePayingUsers(
    rdd: RDD[Tuple2[Object, BSONObject]],
    start: Date,
    end: Date,
    platforms: List[String],
    paymentSystems: List[Int]
  ): RDD[String] = {
    rdd.filter{element =>
      def parseDate(d: String): Option[Date] = {
        try {
          val Format = ThorContext.DateFormat
          Some(new SimpleDateFormat(Format).parse(d))
        } catch {
          case e: Throwable => {
            println("error: " + e.getStackTraceString)
            None
          }
        }
      }

      val dateFilter = parseDate(element._2.get("lowerDate").toString) match {
        case Some(startDate) => {
          startDate.compareTo(start) * end.compareTo(startDate) >= 0
        }
        case _ => false
      }
      if(dateFilter) {
        val platformsFilter = Json.parse(element._2.get("purchasesPerPlatform").toString).as[JsArray].value
          .filter(e => platforms.contains((e \ "platform").as[String]))
          .map(p => {
            // filter only the purchases that are between start and end
            val purchases = (p \ "purchases").as[JsArray].value.toList
            val filter = paymentSystems map {system =>
              purchases.count(pp => {
                /** Checks if the current purchases belongs to the payment system. Then:
                  - Check if number of purchases 
                 **/
                if((pp \ "paymentSystem").as[Double].toInt == system) {
                  val numberCriteria = (pp \ "paymentSystem").as[Double].toInt == 1
                  val purchaseDate = (pp \ "time" \ "$date").as[Date]
                  val dateCriteria = (purchaseDate.equals(start) || purchaseDate.after(start)) && (purchaseDate.equals(end) || purchaseDate.before(end))
                  numberCriteria || dateCriteria
                } else {
                  false
                }
              }) > 0
            }
            ((p \ "platform").as[String], filter.exists(_ == true))
          })
          platformsFilter.exists(_._2)
      } else {
        false
      }
    }.map(_._2.get("userId").toString)
  }
}

class NumberSessionsFirstPurchases(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "payingUsers"
  def outputCollectionType: String = "NumberSessionsFirstPurchase"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: Tuple2[Double,Tuple2[Double, List[Tuple3[String, List[Tuple3[Int, Option[Double], Int]], Int]]]],
    end: Date,
    start: Date,
    companyName: String,
    applicationName: String,
    platforms: List[String],
    paymentSystems: List[Int]
  ): Try[Unit] = {
    try {
      val uri  = MongoClientURI(uriStr)
      val client = MongoClient(uri)
      val collection = client.getDB(uri.database.get)(collectionName)
      val totalResult = if(result._1 > 0) result._2._1 / result._1 else 0.0
      val platformResults = platforms map {p =>
        result._2._2.find(_._1 == p) match {
          case Some(platformValue) => {
            val platformUsers = platformValue._3
            val sumPlatformSessions = platformValue._2.foldLeft(0.0)(_ + _._2.getOrElse(0.0))
            val res = if(platformUsers > 0) sumPlatformSessions / platformUsers else 0.0
            val paymentSystemsResult = paymentSystems map {system =>
              platformValue._2.find(_._1 == system) match {
                case Some(systemInfo) => {
                  val res = if(systemInfo._3 > 0) systemInfo._2.getOrElse(0.0) / systemInfo._3 else 0.0
                  (system, res, systemInfo._3)
                }
                case None => (system, 0.0, 0) //system, result, nrUsers
              }
            }
            (p, platformUsers, res, paymentSystemsResult)
          }
          case None => (p, 0.0, 0, paymentSystems map {(_, 0.0, 0)})
        }
      }
      val obj = MongoDBObject(
        "result" -> totalResult,
        "nrUsers" -> result._1,
        "platforms" -> (platformResults map {p =>
          MongoDBObject(
            "platform" -> p._1,
            "result" -> p._3,
            "nrUsers" -> p._2,
            "paymentSystems" -> (p._4 map {el =>
              MongoDBObject("system" -> el._1, "result" -> el._2, "nrUsers" -> el._3)
            })
          )
        }),
        "lowerDate" -> start,
        "upperDate" -> end
      )
      collection.insert(obj)
      client.close
      new Success
    } catch {
      case e: Exception => new Failure(e)
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

    val collection = getCollectionInput(companyName, applicationName)
    val inputUri = s"${ThorContext.URI}.${collection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val firstTimePayingUsersIds = NumberSessionsFirstPurchases.getFirstTimePayingUsers(sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ),
      start,
      end,
      platforms,
      paymentSystems
    )

    val results: Tuple2[
      Double,
      Tuple2[Double, List[Tuple3[String, List[Tuple3[Int, Option[Double], Int]], Int]]]
      ] = if(firstTimePayingUsersIds.count > 0) {
      val userData = NumberSessionsFirstPurchases.getNumberSessionsFirstPurchasePerUser(
        sc, companyName, applicationName,
        firstTimePayingUsersIds.collect.toList, platforms, paymentSystems
      )

      // [Tuple2[Double, List[Tuple3[String, List[Tuple2[Int, Option[Double]]], Int]]]
      val nrUsers = userData.count.toDouble
      val summedResults = userData.reduce{(acc, current) => {
        val totalSessions = acc._1 + current._1 //sessions
        val platformData = platforms map {platform =>
          def getPaymentSystemsValues(el: List[Tuple3[String, List[Tuple3[Int, Option[Double], Int]], Int]]): List[Tuple3[Int, Double, Int]] = {
            el.find(_._1 == platform) match {
              case Some(platformInfo) => {
                paymentSystems map {system =>
                  platformInfo._2.find(_._1 == system) match {
                    case Some(info) => (system, info._2.getOrElse(0.0), info._3)
                    case None => (system, 0.0, 0)
                  }
                }
              }
              case None => paymentSystems map {(_, 0.0, 0)}
            }
          }

          def updatePlatformUsers(el: List[Tuple3[String, List[Tuple3[Int, Option[Double], Int]], Int]]): Int = {
            el.find(p => p._1 == platform) match {
              case Some(v) => v._3 +1
              case None => 0
            }
          }

          val updatedPaymentSystems = getPaymentSystemsValues(acc._2).zip(getPaymentSystemsValues(current._2)).map{el =>
            (el._1._1, Some(el._1._2 + el._2._2), el._1._3 + el._2._3)
            //(paymentSystems, nrSessions
          }
          (platform, updatedPaymentSystems, updatePlatformUsers(current._2))
        }
        (totalSessions, platformData)
      }}
      (nrUsers, summedResults)
    } else {
      log.info("Count is zero")
      (
        0.0,
        (0.0, platforms map {(_, paymentSystems map {(_, Some(0.0), 0)} , 0 ) })
      )
    }

    saveResultToDatabase(
      ThorContext.URI,
      getCollectionOutput(companyName, applicationName),
      results,
      end,
      start,
      companyName,
      applicationName,
      platforms,
      paymentSystems
    ) match {
      case Success(_) => promise.success()
      case Failure(e) => promise.failure(e)
    }
    promise.future
  }

  def kill = stop(self)

  //TODO
  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms, paymentSystems) => {
      try {
        log.info(s"core job ended ${sender.toString}")
        updateCompletedDependencies(sender)
        if(dependenciesCompleted) {
          log.info("execute job")
          executeJob(companyName, applicationName, lower, upper, platforms, paymentSystems) map { arpu =>
            log.info("Job completed successful")
            onJobSuccess(companyName, applicationName, "Number Sessions First Purchase")
          } recover {
            case ex: Exception => onJobFailure(ex, "Number Sessions First Purchase")
          }
        }
      } catch {
        case ex: Exception => {
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - NUMBER SESSIONS 1ST PURCHASE", ex.getStackTraceString)
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

