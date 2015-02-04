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

object AvgTimeBetweenPurchases {
  def props(sc: SparkContext): Props = Props(new AvgTimeBetweenPurchases(sc))

  def mapRDD(rdd: RDD[Tuple2[Object, BSONObject]], platforms: List[String]) = {
    rdd map {element =>
      def calculateSumTimeBetweenPurchases(lst: List[JsValue]): Double = {
        def getNumberSecondsBetweenDates(d1: Date, d2: Date): Float = {
          (new LocalDate(d2).toDateTimeAtCurrentTime.getMillis - new LocalDate(d1).toDateTimeAtCurrentTime().getMillis) / 1000
        }

        def parseDate(json: JsValue, key: String): Date = {
          val dateStr = (json \ key \ "$date").as[String]
          val ops = new StringOps(dateStr)
          new SimpleDateFormat("yyyy-MM-dd").parse(ops.take(ops.indexOf('T')))
        }

        val purchases = lst.zipWithIndex
        if(purchases.size > 1) {
          purchases.foldLeft(0.0){(acc, current) => {
            val index = current._2
            if(index < purchases.size) {
              acc + getNumberSecondsBetweenDates(
                parseDate(current._1, "time"),
                parseDate(purchases(index+1)._1, "time")
              )
            } else acc
          }}
        } else 0.0
      }

      val purchases = Json.parse(element._2.get("purchases").toString).as[JsArray].value.toList
      val sumTimeAllPurchases = calculateSumTimeBetweenPurchases(purchases)
      val totalPurchases = purchases.size

      val purchasesPerPlatform = Json.parse(element._2.get("purchasesPerPlatform").toString).as[JsArray].value
      val sumTimeAllPurchasesPerPlatform = platforms map {platform =>
        val timeAndNrPurchases = purchasesPerPlatform.find(e => (e \ "platform").as[String] == platform) match {
          case Some(platformData) => {
            val p = (platformData \ "purchases").as[JsArray].value.toList
            (p.size, calculateSumTimeBetweenPurchases(p))
          }
          case _ => (0, 0.0)
        }
        (platform, timeAndNrPurchases._1, timeAndNrPurchases._2)
      }
      (sumTimeAllPurchases, sumTimeAllPurchasesPerPlatform)
    }
  }

  def reduceRDD(rdd: RDD[Tuple2[Double, List[Tuple3[String, Int, Double]]]], platforms: List[String]) = {
    rdd.reduce{(acc, current) => {
      val totalTime = acc._1 + current._1
      val platformData = platforms map {platform =>
        val accumPlatformData = acc._2.find(_._1 == platform).get
        val pData = current._2.find(_._1 == platform).get
        val updatedNumberPurchases = accumPlatformData._2 + pData._2
        val updatedTime = accumPlatformData._3 + pData._3
        (platform, updatedNumberPurchases, updatedTime)
      }
      (totalTime, platformData)
    }}
  }
}

class AvgTimeBetweenPurchases(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "AvgTimeBetweenPurchases"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: Tuple2[Double, List[Tuple2[String, Double]]],
    start: Date,
    end: Date,
    platforms: List[String]
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val platformResults = if(result._2.isEmpty) {
      platforms map {p => MongoDBObject("platform" -> p, "res" -> 0.0)}
    } else {
      result._2 map {el => MongoDBObject("platform" -> el._1, "res" -> el._2)}
    }
    val obj = MongoDBObject(
      "total" -> result._1,
      "platforms" -> platformResults,
      "lowerDate" -> start,
      "upperDate" -> end
    )
    collection.insert(obj)
    client.close
  }

  private def calculateAverageTimeBetweenPurchases(
    collectionName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ) = {
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
    val payingUsersRDD = filterRDDByDateFields(("lowerDate", "upperDate"), rdd, start, end, sc)

    if(payingUsersRDD.count > 0) {
      AvgTimeBetweenPurchases.reduceRDD(
        AvgTimeBetweenPurchases.mapRDD(payingUsersRDD, platforms), platforms
      )
    } else {
      // Empty result
      (0.0, platforms map {(_, 0, 0.0)})
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

    def getNumberSecondsBetweenDates(d1: Date, d2: Date): Float = {
      (new LocalDate(d2).toDateTimeAtCurrentTime.getMillis - new LocalDate(d1).toDateTimeAtCurrentTime().getMillis) / 1000
    }

    val data = calculateAverageTimeBetweenPurchases(
      s"${companyName}_payingUsers_${applicationName}",
      start,
      end,
      platforms
    )
    val totalPurchases = data._2.foldLeft(0.0){_ + _._2}
    val totalTimeBetweenPurchases = if(totalPurchases > 0) data._1 / totalPurchases else 0.0
    val timeBetweenPurchasesPerPlatform = platforms map {platform =>
      val platformData = data._2.find(_._1 == platform).get
      val result = if(platformData._2 > 0) platformData._3 / platformData._2 else 0.0
      (platform, result)
    }

    val finalResult = (totalTimeBetweenPurchases, timeBetweenPurchasesPerPlatform)

    saveResultToDatabase(
      ThorContext.URI,
      getCollectionOutput(companyName, applicationName),
      finalResult,
      start,
      end,
      platforms
    )

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
            onJobSuccess(companyName, applicationName, "Average Time Between Purchases, platforms")
          } recover {
            case ex: Exception => onJobFailure(ex, "Average Time Between Purchases")
          }
        }
      } catch {
        case ex: Exception => {
          log.error(ex.getStackTraceString)
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - AVG TIME BETWEEN PURCHASES", ex.getStackTraceString)
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

