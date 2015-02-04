package wazza.thor.jobs

import com.mongodb.casbah.Imports._
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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import scala.collection.immutable.StringOps
import wazza.thor.NotificationMessage
import wazza.thor.NotificationsActor
import wazza.thor.messages._

object ActiveUsers {

  def props(ctx: SparkContext, dependants: List[ActorRef]): Props = Props(new ActiveUsers(ctx, dependants))
}

class ActiveUsers(
  ctx: SparkContext,
  d: List[ActorRef]
) extends Actor with ActorLogging with CoreJob {
  import context._

  dependants = d

  override def inputCollectionType: String = "mobileSessions"
  override def outputCollectionType: String = "activeUsers"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    payingUsers: Int,
    lowerDate: Date,
    upperDate: Date
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val result = MongoDBObject(
      "result" -> payingUsers,
      "lowerDate" -> lowerDate,
      "upperDate" -> upperDate
    )
    collection.insert(result)
    client.close()
  }

  def executeJob(
    inputCollection: String,
    outputCollection: String,
    lowerDate: Date,
    upperDate: Date,
    platforms: List[String]
  ): Future[Unit] = {

    val promise = Promise[Unit]
    val inputUri = s"${ThorContext.URI}.${inputCollection}"
    val outputUri = s"${ThorContext.URI}.${outputCollection}"
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

    // Creates an RDD with data of mobile platform
    val rdds = getRDDPerPlatforms("startTime", platforms, rdd, lowerDate, upperDate, ctx)

    // Calculates results per platform
    val platformResults = rdds map {rdd =>
      if(rdd._2.count() > 0) {
        val activeUsers = rdd._2.map {arg => {
          (arg._2.get("userId"), 1)
        }}.groupByKey.count
        new PlatformResults(rdd._1, activeUsers)
      } else {
        new PlatformResults(rdd._1, 0.0)
      }
    }

    val activeUsers = platformResults.foldLeft(0.0)(_ + _.res)
    val results = new Results(activeUsers, platformResults, lowerDate, upperDate)
    saveResultToDatabase(ThorContext.URI, outputCollection, results)
    promise.success()
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case InitJob(companyName ,applicationName, platforms, lowerDate, upperDate) => {
      try {
        log.info(s"InitJob received - $companyName | $applicationName | $lowerDate | $upperDate")
        supervisor = sender
        executeJob(
          getCollectionInput(companyName, applicationName),
          getCollectionOutput(companyName, applicationName),
          lowerDate,
          upperDate,
          platforms
        ) map {res =>
          log.info("Job completed successful")
          onJobSuccess(companyName, applicationName, self.path.name, lowerDate, upperDate, platforms)
        } recover {
          case ex: Exception => {
            log.error("Job failed")
            onJobFailure(ex, "Active Users")
          }
        }
      } catch {
        case ex: Exception => {
          log.error(ex.getStackTraceString)
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - ACTIVE USERS", ex.getStackTraceString)
          onJobFailure(ex, self.path.name)
        }
      }
    }

    /** Must wait for all childs to finish **/
    case JobCompleted(jobType, status) => {
      childJobsCompleted = childJobsCompleted :+ jobType
      if(childJobsCompleted.size == dependants.size) {
        log.info("All child jobs have finished")
        supervisor ! new JobCompleted(self.path.name, new Success)
        kill
      }
    }
  }
}

