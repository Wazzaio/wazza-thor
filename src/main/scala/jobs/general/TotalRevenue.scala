package wazza.thor.jobs

import com.mongodb.casbah.Imports._
import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import scala.collection.immutable.StringOps
import wazza.thor.NotificationMessage
import wazza.thor.NotificationsActor
import wazza.thor.messages._
import akka.actor.PoisonPill

object TotalRevenue {

  def props(ctx: SparkContext, dependants: List[ActorRef]): Props = Props(new TotalRevenue(ctx, dependants))
}

class TotalRevenue(
  ctx: SparkContext,
  d: List[ActorRef]
) extends Actor with ActorLogging with CoreJob  {
  import context._

  dependants = d

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "TotalRevenue"

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

    // Creates RDD based on configuration
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
    val rdds = getRDDPerPlatforms("time", platforms, rdd, lowerDate, upperDate, ctx)

    // Calculates results per platform
    val platformResults = rdds map {rdd =>
      if(rdd._2.count() > 0) {
        val totalRevenue = rdd._2.map(arg => {
          val price = arg._2.get("price").toString.toDouble
          price
        }).sum
        new PlatformResults(rdd._1, totalRevenue)
      } else {
        null
      }
    }

    // Calculates total results and persist them
    val results = if(!platformResults.exists(_ == null)) {
      val totalRevenue = platformResults.foldLeft(0.0)(_ + _.res)
      new Results(totalRevenue, platformResults, lowerDate, upperDate)
      
    } else {
      log.info("Count is zero")
      new Results(0.0, platforms map {new PlatformResults(_, 0.0)}, lowerDate, upperDate)
    } 
    saveResultToDatabase(ThorContext.URI, outputCollection, results)
    promise.success()
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case InitJob(companyName ,applicationName, platforms, lowerDate, upperDate) => {
      log.info(s"InitJob received - $companyName | $applicationName | $lowerDate | $upperDate")
      supervisor = sender
      try {
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
          onJobSuccess(companyName, applicationName, "Total Revenue", lowerDate, upperDate, platforms)
        } recover {
          case ex: Exception => {
            log.error("Job failed")
            onJobFailure(ex, "Total Revenue")
          }
        }
      } catch {
        case ex: Exception => {
          log.error(ex.getStackTraceString)
          NotificationsActor.getInstance ! new NotificationMessage("SPARK ERROR - TOTAL REVENUE", ex.getStackTraceString)
          onJobFailure(ex, self.path.name)
        }
      }
    }
    /** Must wait for all childs to finish **/
    case JobCompleted(jobType, status) => {
      childJobsCompleted = childJobsCompleted :+ jobType
      if(childJobsCompleted.size == dependants.size) {
        log.info("All child jobs have finished")
        supervisor ! new JobCompleted(self.path.name, new wazza.thor.messages.Success)
        kill
      }
    }
  }
}
