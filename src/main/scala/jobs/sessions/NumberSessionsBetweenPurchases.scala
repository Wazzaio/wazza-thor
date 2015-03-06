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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps
import wazza.thor.NotificationMessage
import wazza.thor.NotificationsActor
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import org.joda.time.LocalDate

object NumberSessionsBetweenPurchases {
  def props(sc: SparkContext): Props = Props(new NumberSessionsBetweenPurchases(sc))

  def getSessionsBetweenPurchasesPerUser(
    sc: SparkContext,
    payingUsersIds: List[String],
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): RDD[Tuple2[Object, Tuple2[Double, List[Tuple2[String, Double]]]]] = {
    val uri = ThorContext.URI
    val collectionName = s"${companyName}_mUsers_${applicationName}"
    val inputUri = s"${uri}.${collectionName}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")
    val emptyRDD = sc.emptyRDD[Tuple2[Object, Tuple2[Double, List[Tuple2[String, Double]]]]]

    val rdd = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ).filter(element => {
      payingUsersIds.contains(element._2.get("userId").toString)
    })

    if(rdd.count() > 0) {
      rdd.map(element => {
        def parseDate(json: JsValue, key: String): Date = {
          val dateStr = (json \ key \ "$date").as[String].replace("T", " ").replace("Z", "")
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr)
        }

        def calculate(purchases: List[(JsValue, Int)], sessions: List[JsValue]) = {
          def getNumberSessionsBetweenDates(sessions: List[JsValue], start: Date, end: Date): Double = {
            val nrSessions = sessions.count(s => {
              val sessionDate = parseDate(s, "startTime")
                (sessionDate.after(start) || sessionDate.equals(start)) && (sessionDate.before(end) || sessionDate.equals(end))
            }).toDouble
            if(nrSessions > 2.0)
              nrSessions -2.0 /** Purchase sessions dont count **/
            else
              nrSessions
          }
          purchases.foldLeft(0.0)((acc, current) => {
            val index = current._2
            if(index < purchases.size-1) {
              acc + getNumberSessionsBetweenDates(
                sessions,
                parseDate(current._1, "time"),
                parseDate(purchases(index+1)._1, "time")
              )
            } else acc
          })
        }

        val purchases = Json.parse(element._2.get("purchases").toString).as[JsArray].value.toList.filter(p => {
          val purchaseDate = parseDate(p, "time")
          (purchaseDate.after(start) || purchaseDate.equals(start)) && (purchaseDate.before(end) || purchaseDate.equals(end))
        }).zipWithIndex

        val results = if(purchases.size > 1) {
          val sessions = Json.parse(element._2.get("sessions").toString).as[JsArray].value.toList
          val total = calculate(purchases, sessions)
          val platformResults = platforms map {platform =>
            val platformSessions = sessions.filter(s => (s \ "platform").as[String] == platform)
            val platformPurchases = purchases.filter(p => (p._1 \ "platform").as[String] == platform)
            (platform, calculate(platformPurchases, platformSessions))
          }
          (total, platformResults)
        } else {
          (0.0, platforms map {(_, 0.0)})
        }
        (element._2.get("userId").toString, results)
      })
    } else {
      emptyRDD
    }
  }

  def getTotalSessionsBetweenPurchases(
    rdd: RDD[Tuple2[Object, Tuple2[Double, List[Tuple2[String, Double]]]]],
    platforms: List[String]
  ): Tuple2[Long, Tuple2[Double, List[Tuple2[String, Double]]]] = {
    val numberUsers = rdd.count
    if(numberUsers > 0) {
      val summedResults = rdd.values.reduce{(acc, current) => {
        val totalSessions = acc._1 + current._1
        val platformData = platforms map {p =>
          val accumPlatformData = acc._2.find(_._1 == p).get
          val pData = current._2.find(_._1 == p).get
          (p, accumPlatformData._2 + pData._2)
        }
        (totalSessions, platformData)
      }}
      (numberUsers, summedResults)
    } else {
      (0, (0.0, platforms map {(_, 0.0)}))
    }
  }
}

class NumberSessionsBetweenPurchases(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "NumberSessionsBetweenPurchases"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: Tuple2[Long, Tuple2[Double, List[Tuple2[String, Double]]]],
    start: Date,
    end: Date,
    platforms: List[String]
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val totalAverage = if(result._1 > 0.0) result._2._1 / result._1 else 0.0
    val platformsAverage = platforms map {p =>
      val value = result._2._2.find(_._1 == p).get._2
      val res = if(result._1 > 0.0) value / result._1 else 0.0
      (p, res)
    }

    val platformResults = if(result._2._2.isEmpty) {
      platforms map {p => MongoDBObject("platform" -> p, "res" -> 0.0, "totalSessions" -> 0.0)}
    } else {
      result._2._2 map {el =>
        MongoDBObject(
          "platform" -> el._1,
          "res" -> el._2,
          "totalSessions" -> platformsAverage.find(_._1 == el._1).get._2)
      }
    }

    val obj = MongoDBObject(
      "result" -> totalAverage,
      "totalSessions" -> result._2._1,
      "numberUsers" -> result._1.toInt,
      "platforms" -> platformResults,
      "lowerDate" -> start,
      "upperDate" -> end
    )

    collection.insert(obj)
    client.close
  }

  private def calculateNumberSessionsBetweenPurchases(
    companyName: String,
    applicationName: String,
    collectionName: String, 
    start: Date, 
    end: Date, 
    platforms: List[String]
  ): Tuple2[Long, Tuple2[Double, List[Tuple2[String, Double]]]] = {
    val uri = ThorContext.URI
    val inputUri = s"${uri}.${collectionName}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val rdd = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )
    val payingUsersIds = filterRDDByDateFields(("lowerDate", "upperDate"), rdd, start, end, sc).map(element => {
      element._2.get("userId").toString
    }).collect.toList

    NumberSessionsBetweenPurchases.getTotalSessionsBetweenPurchases(
      NumberSessionsBetweenPurchases.getSessionsBetweenPurchasesPerUser(
        sc, payingUsersIds, companyName, applicationName, start, end, platforms
      ),
      platforms
    )
  }

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): Future[Unit] = {
    val promise = Promise[Unit]
    val data = calculateNumberSessionsBetweenPurchases(
      companyName,
      applicationName,
      s"${companyName}_payingUsers_${applicationName}",
      start,
      end,
      platforms
    )

    saveResultToDatabase(
      ThorContext.URI,
      getCollectionOutput(companyName, applicationName),
      data,
      start,
      end,
      platforms
    )

    promise.success()
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
            onJobSuccess(companyName, applicationName, "Number Sessions Between Purchases")
          } recover {
            case ex: Exception => onJobFailure(ex, "Number Sessions Between Purchases")
          }
        }
      } catch {
        case ex: Exception => {
          log.error(ex.getStackTraceString)
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - NUMBER SESSIONS BETWEEN PURCHASES", ex.getStackTraceString)
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

