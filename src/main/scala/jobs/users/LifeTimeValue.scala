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
import wazza.thor.NotificationMessage
import wazza.thor.NotificationsActor
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import org.joda.time.LocalDate

//TODO missing user retention rate
object LifeTimeValue {
  def props(sc: SparkContext): Props = Props(new LifeTimeValue(sc))
}
sealed case class NrSessionsUserPlatformData(total: Double, platform: String)
sealed case class NrSessionsPerUserData(nrUsers: Double, total: Double, platforms: List[NrSessionsUserPlatformData])
object NrSessionsPerUserData {
  def apply: NrSessionsPerUserData = new NrSessionsPerUserData(0, 0, List[NrSessionsUserPlatformData]())
}

sealed case class LifeTimeValuePlatformsResult(value: Double, platform: String)
object LifeTimeValuePlatformsResult {

  implicit def toBSON(ltv: LifeTimeValuePlatformsResult): MongoDBObject = {
    MongoDBObject("res" -> ltv.value, "platform" -> ltv.platform)
  }

  implicit def toListBSON(lst: List[LifeTimeValuePlatformsResult]): List[MongoDBObject] = {
    lst.map(toBSON(_))
  }
}

sealed case class LifeTimeValueResult(total: Double, platforms: List[LifeTimeValuePlatformsResult])
object LifeTimeValueResult {

  implicit def toBSON(ltv: LifeTimeValueResult): MongoDBObject = {
    MongoDBObject("total" -> ltv.total, "platforms" -> LifeTimeValuePlatformsResult.toListBSON(ltv.platforms))
  }

  def default(platforms: List[String]): LifeTimeValueResult = {
    new LifeTimeValueResult(0, platforms map {new LifeTimeValuePlatformsResult(0,_)})
  }
  
}

class LifeTimeValue(ctx: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  private val ProfitMargin = 0.70
  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "LifeTimeValue"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: LifeTimeValueResult,
    end: Date,
    start: Date,
    platforms: List[String]
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val platformResults = if(result.platforms.isEmpty) {
      platforms.map {p => MongoDBObject("platform" -> p, "res" -> 0.0)}
    } else {
      LifeTimeValuePlatformsResult.toListBSON(result.platforms)
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

  private def getARPU(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): Option[JsValue] = {
    val collection = s"${companyName}_Arpu_${applicationName}"
    val uri = ThorContext.URI
    val inputUri = s"${uri}.${collection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val arpuRDD = filterRDDByDateFields(
      ("lowerDate", "upperDate"),
      ctx.newAPIHadoopRDD(
        jobConfig,
        classOf[com.mongodb.hadoop.MongoInputFormat],
        classOf[Object],
        classOf[BSONObject]
      ),
      start,
      end,
      ctx
    )

    if(arpuRDD.count > 0) {
      Some(arpuRDD.map{t => Json.parse(t._2.toString)}.collect.toList.head)
    } else {
      log.info("No ARPU values found")
      None
    }
  }

  private def getNumberSessionsPerUser(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): Option[NrSessionsPerUserData] = {
    val collection = s"${companyName}_numberSessionsPerUser_${applicationName}"
    val uri = ThorContext.URI
    val inputUri = s"${uri}.${collection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val nrSessionsPerUserRDD = filterRDDByDateFields(
      ("lowerDate", "upperDate"),
      ctx.newAPIHadoopRDD(
        jobConfig,
        classOf[com.mongodb.hadoop.MongoInputFormat],
        classOf[Object],
        classOf[BSONObject]
      ),
      start,
      end,
      ctx
    )

    if(nrSessionsPerUserRDD.count > 0) {
      val result = nrSessionsPerUserRDD.map{t =>
        val nrSessionsUserList = Json.parse(t._2.get("nrSessionsPerUser").toString).as[JsArray].value
        nrSessionsUserList.foldLeft(NrSessionsPerUserData.apply){(res, current) => {
          val total = res.total + (current \ "total").as[Double]
          val nrUsers = res.nrUsers +1
          val platformResults = platforms map {platform =>
            val elementPlatforms = (current \ "platforms").as[JsArray].value
            val platformSessionsPerUser = elementPlatforms.find(e => (e \ "platform").as[String] == platform) match {
              case Some(value) => (value \ "sessions").as[Double]
              case None => 0
            }
            res.platforms.find(_.platform == platform) match {
              case Some(oldPlatformResults) => {
                new NrSessionsUserPlatformData(platformSessionsPerUser + oldPlatformResults.total, platform)
              }
              case None => new NrSessionsUserPlatformData(platformSessionsPerUser, platform)
            }
          }
          new NrSessionsPerUserData(nrUsers, total, platformResults)
        }}
      }.reduce{(res, current) =>
        val nrUsers = res.nrUsers + current.nrUsers
        val total = res.total + current.total
        val platformResults = platforms map {platform =>
          val resPlatform = res.platforms.find(_.platform == platform).get.total
          val currentPlatform = current.platforms.find(_.platform == platform).get.total
          new NrSessionsUserPlatformData(resPlatform + currentPlatform, platform)
        }
        new NrSessionsPerUserData(nrUsers, total, platformResults)
      }
      Some(result)
    } else {
      log.info("No Number of Sessions Per User values found")
      None
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

    val optARPU = getARPU(companyName, applicationName, start, end, platforms)
    val optNrSessionsUser = getNumberSessionsPerUser(companyName, applicationName, start, end, platforms)

    // TODO - missing user retention rate
    val result = (optARPU, optNrSessionsUser) match {
      case (Some(arpuJson), Some(nrSessionsUser)) => {
        val arpu = (arpuJson \ "result").as[Double]
        val arpuPlatformResults = (arpuJson \ "platforms").as[JsArray].value
        val arpuPerPlatform = platforms map {platform =>
          val value = arpuPlatformResults.find(e => (e \ "platform").as[String] == platform) match {
            case Some(data) => (data \ "res").as[Double]
            case None => 0
          }
          (platform, value)
        }

        val totalLTV = arpu * nrSessionsUser.total * ProfitMargin
        val platformsLTV = (arpuPerPlatform zip nrSessionsUser.platforms) map {element =>
          val arpu = element._1._2
          val nrSessions = element._2.total
          new LifeTimeValuePlatformsResult(arpu * nrSessions * ProfitMargin, element._1._1)
        }

        new LifeTimeValueResult(totalLTV, platformsLTV)
      }
      case _ => {
        log.info("Default value for LTV")
        LifeTimeValueResult.default(platforms)
      }
    }

    saveResultToDatabase(
      ThorContext.URI,
      getCollectionOutput(companyName, applicationName),
      result,
      end,
      start,
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
            onJobSuccess(companyName, applicationName, self.path.name)
          } recover {
            case ex: Exception => onJobFailure(ex, self.path.name)
          }
        }
      } catch {
        case ex: Exception => {
          log.error(ex.getStackTraceString)
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - LIFE TIME VALUE", ex.getStackTraceString)
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

