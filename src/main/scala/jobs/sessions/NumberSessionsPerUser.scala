package wazza.thor.jobs

import com.mongodb.casbah.Imports._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import scala.collection.mutable.ListBuffer
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import scala.concurrent._
import akka.actor.{Actor, ActorLogging, Props, ActorRef}
import scala.collection.mutable.ArrayBuffer
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import scala.collection.JavaConversions._
import scala.util.{Success,Failure}
import scala.collection.immutable.StringOps
import wazza.thor.messages._

object NumberSessionsPerUser {

  def props(ctx: SparkContext, dependants: List[ActorRef]): Props = Props(new NumberSessionsPerUser(ctx, dependants))
}

sealed case class NrSessionsPerUser(userId: String, nrSessions: Int)
sealed case class PlatformNrSessionsPerUser(platform: String, sessionsPerUser: List[NrSessionsPerUser])
sealed case class PlatformSessions(platform: String, sessions: Int)
sealed case class SessionsPerUserResult(userId: String, total: Int, platforms: List[PlatformSessions])

class NumberSessionsPerUser(
  ctx: SparkContext,
  d: List[ActorRef]
) extends Actor with ActorLogging with CoreJob {
  import context._

  dependants = d

  def inputCollectionType: String = "mobileSessions"
  def outputCollectionType: String = "numberSessionsPerUser"

  private def platformSessionToBson(platformSession: PlatformSessions): MongoDBObject = {
    MongoDBObject(
      "platform" -> platformSession.platform,
      "sessions" -> platformSession.sessions
    )
  }

  private def sessionsPerUserResultToBson(result: SessionsPerUserResult): MongoDBObject = {
    MongoDBObject(
      "userId" -> result.userId,
      "total" -> result.total,
      "platforms" -> result.platforms.map{platformSessionToBson(_)}
    )
  }

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    nrSessionsPerUser: List[SessionsPerUserResult],
    lowerDate: Date,
    upperDate: Date
  ) = {
    val uri = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get).getCollection(collectionName)
    val model = MongoDBObject(
      "nrSessionsPerUser" -> (nrSessionsPerUser map {sessionsPerUserResultToBson(_)}),
      "lowerDate" -> lowerDate,
      "upperDate" -> upperDate
    )
    collection.insert(model)
    client.close()
  }

  private def executeJob(
    inputCollection: String,
    outputCollection: String,
    lowerDate: Date,
    upperDate: Date,
    companyName: String,
    applicationName: String,
    platforms: List[String]
  ): Future[Unit] = {

    val promise = Promise[Unit]
    val uri = ThorContext.URI
    val inputUri = s"${uri}.${inputCollection}"
    val outputUri = s"${uri}.${outputCollection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.output.uri", outputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val rdd = ctx.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )

    val rdds = getRDDPerPlatforms("startTime", platforms, rdd, lowerDate, upperDate, ctx)

    val platformResults = rdds map {rdd =>
      val numberSessionsPerUser = if(rdd._2.count() > 0) {
        rdd._2.map(arg => {
          (arg._2.get("userId").toString, 1)
        }).reduceByKey(_ + _).map{r =>
          new NrSessionsPerUser(r._1, r._2)
        }.collect.toList
      } else Nil
      new PlatformNrSessionsPerUser(rdd._1, numberSessionsPerUser)
    }

    if(!platformResults.isEmpty) {
      val results = platformResults.foldLeft(List.empty[SessionsPerUserResult]){(lst, element) => {
        var buffer: ListBuffer[SessionsPerUserResult] = lst.to[ListBuffer]
        for(spu <- element.sessionsPerUser) {
          if(!buffer.exists(_.userId == spu.userId)) {
            buffer += new SessionsPerUserResult(
              spu.userId,
              spu.nrSessions,
              List(new PlatformSessions(element.platform, spu.nrSessions))
            )
          } else {
            val oldInfo = buffer.find(_.userId == spu.userId).get
            val newTotal = oldInfo.total + spu.nrSessions
            val updatedPlatforms = {
              // If no element exists for this platform, create it
              if(!oldInfo.platforms.exists(_.platform == element.platform)) {
                oldInfo.platforms :+ (new PlatformSessions(element.platform, spu.nrSessions))
              } else {
                // If an element exists, update it
                val platform = oldInfo.platforms.find(_.platform == element.platform).get
                val updatedPlatform = new PlatformSessions(element.platform, platform.sessions + spu.nrSessions)
                oldInfo.platforms.updated(oldInfo.platforms.indexOf(platform), updatedPlatform)
              }
            }
            val updatedSessionsPerUserResult = new SessionsPerUserResult(spu.userId, newTotal, updatedPlatforms)
            buffer.update(buffer.indexOf(oldInfo), updatedSessionsPerUserResult)
          }
        }
        buffer.toList
      }}

      saveResultToDatabase(ThorContext.URI, outputCollection, results, lowerDate, upperDate)
      promise.success()
    }

    promise.future
  }

  def kill = stop(self)

  def receive = {
    case InitJob(companyName ,applicationName, platforms, lowerDate, upperDate) => {
      log.info(s"InitJob received - $companyName | $applicationName | $lowerDate | $upperDate")
      supervisor = sender
      executeJob(
        getCollectionInput(companyName, applicationName),
        getCollectionOutput(companyName, applicationName),
        lowerDate,
        upperDate,
        companyName,
        applicationName,
        platforms
      ) map {res =>
        log.info("Job completed successful")
        onJobSuccess(companyName, applicationName, "Number Sessions Per User", lowerDate, upperDate, platforms)
      } recover {
        case ex: Exception => {
          log.error("Job failed")
          onJobFailure(ex, "Number Sessions Per User")
        }
      }
    }
    /** Must wait for all childs to finish **/
    case JobCompleted(jobType, status) => {
      childJobsCompleted = childJobsCompleted :+ jobType
      if(childJobsCompleted.size == dependants.size) {
        log.info("All child jobs have finished")
        supervisor ! JobCompleted(jobType, new wazza.thor.messages.Success)
        kill
      }
    }
  }
}

