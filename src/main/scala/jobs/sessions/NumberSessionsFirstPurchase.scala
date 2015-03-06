package wazza.thor.jobs

import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import org.apache.spark.rdd.RDD
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
    platforms: List[String]
  ): RDD[Tuple2[Double, List[Tuple2[String, Double]]]] = {

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
          val dateStr = (p \ "time" \ "$date").as[String].replace("T", " ").replace("Z", "")
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr)
        }).head

        val sessionsToPurchase = Json.parse(userInfo._2.get("sessions").toString).as[JsArray].value.toList.count(s => {
          val sessionDate = {
            val dateStr = (s \ "startTime" \ "$date").as[String].replace("T", " ").replace("Z", "")
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr)
          }
          sessionDate.before(purchaseDate) || sessionDate.equals(purchaseDate)
        }).toDouble

        //Calculate number of sessions to 1st purchase per platform
        val platformData = platforms map {p =>
          val purchases = Json.parse(userInfo._2.get("purchases").toString).as[JsArray].value.toList
          val purchaseDateOpt = if(purchases.size == 1) {
            purchases.find(pp => (pp \ "platform").as[String] == p) match {
              case Some(purchase) => {
                val dateStr = (purchase \ "time" \ "$date").as[String].replace("T", " ").replace("Z", "")
                Some(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr))
              }
              case None => None
            }
          } else {
            None
          }

          //Date is an Option[Date] so if resolves to None, the current value is not considered
          purchaseDateOpt match {
            case Some(purchaseDate) => {
              val sessionsToPurchase = Json.parse(userInfo._2.get("sessions").toString).as[JsArray].value.toList.count(s => {
                val sessionDate = {
                  val dateStr = (s \ "startTime" \ "$date").as[String].replace("T", " ").replace("Z", "")
                  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr)
                }
                val dateValidation = sessionDate.before(purchaseDate) || sessionDate.equals(purchaseDate)
                val platformValidation = (s \ "platform").as[String] == p
                dateValidation && platformValidation
              })
              (p, sessionsToPurchase.toDouble)
            }
            case None => {
              (p, 0.0)
            }
          }
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
    platforms: List[String]
  ): RDD[String] = {
    rdd.filter{element =>
      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toDouble.toLong) } catch { case _: Throwable => None }
      }

      val dateFilter = parseFloat(element._2.get("lowerDate").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(start) * end.compareTo(startDate) >= 0
        }
        case _ => false
      }
      if(!dateFilter /** DEBUG **/) {
        val purchasesFilter = Json.parse(element._2.get("purchases").toString).as[JsArray].value.size == 1
        val platformsFilter = Json.parse(element._2.get("purchasesPerPlatform").toString).as[JsArray].value
          .filter(e => platforms.contains((e \ "platform").as[String]))
          .map(p => {
            val filter = (p \ "purchases").as[JsArray].value.size == 1
            ((p \ "platform").as[String], filter)
          })
        if(purchasesFilter) {
          true
        } else {
          platformsFilter.exists(_._2)
        }
      } else {
        false
      }
      true
    }.map(_._2.get("userId").toString)
  }
}

class NumberSessionsFirstPurchases(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

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
      "avgTimeFirstPurchase" -> result,
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
    end: Date,
    platforms: List[String]
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
      platforms
    )

    if(firstTimePayingUsersIds.count > 0) {
      println(firstTimePayingUsersIds.collect.toList)
      val userData = NumberSessionsFirstPurchases.getNumberSessionsFirstPurchasePerUser(
        sc, companyName, applicationName,
        firstTimePayingUsersIds.collect.toList, platforms
      )

      val nrUsers = userData.count.toDouble
      println(userData.collect.toList)
      val summedResults = userData.reduce{(acc, current) => {
        val total = acc._1 + current._1
        val platformData = platforms map {platform =>
          def getPlatformValue(el: List[Tuple2[String, Double]]): Double = {
            el.find(p => p._1 == platform) match {
              case Some(v) => v._2
              case None => 0.0
            }
          }
          val value = getPlatformValue(current._2)
          val platformAcc = getPlatformValue(acc._2)
          (platform, value + platformAcc)
        }
        (total, platformData)
      }}

      println(summedResults)
      promise.success()
    } else {
      log.info("Count is zero")
      //TODO default zero result
      //promise.failure(new Exception)
    }
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms) => {
      try {
        log.info(s"core job ended ${sender.toString}")
        updateCompletedDependencies(sender)
        if(dependenciesCompleted) {
          log.info("execute job")
          executeJob(companyName, applicationName, lower, upper, platforms) map { arpu =>
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

