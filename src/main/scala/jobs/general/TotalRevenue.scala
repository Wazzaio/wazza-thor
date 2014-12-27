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

  private case class PlatformResults(platform: String, res: Double)
  private case class Results(
    totalRevenue: Double,
    platforms: List[PlatformResults],
    lowerDate: Date,
    upperDate: Date
  )

  private def resultsToMongo(results: Results): MongoDBObject = {
    MongoDBObject(
      "totalRevenue" -> results.totalRevenue,
      "platforms" -> (results.platforms map {p => MongoDBObject("platform" -> p.platform, "res" -> p.res)}),
      "lowerDate" -> results.lowerDate,
      "upperDate" -> results.upperDate
    )
  }

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "TotalRevenue"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    results: Results,
    companyName: String,
    applicationName: String
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get).getCollection(collectionName)
    collection.insert(resultsToMongo(results))
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
    val emptyRDD = ctx.emptyRDD

    // Creates an RDD with data of mobile platform
    val rdds = platforms map {p =>
      val filteredRDD = rdd.filter(t => {
        def parseDate(d: String): Option[Date] = {
          try {
            val Format = "E MMM dd HH:mm:ss Z yyyy"
            Some(new SimpleDateFormat(Format).parse(d))
          } catch {case _: Throwable => None }
        }

        val dateStr = t._2.get("time").toString
        val t2 = parseDate(dateStr)
        val time = parseDate(dateStr) match {
          case Some(startDate) => {
            startDate.compareTo(lowerDate) * upperDate.compareTo(startDate) >= 0
          } 
          case _ => false
        }
        val platform = (Json.parse(t._2.get("device").toString) \ "osType").as[String] == p
        time && platform
      })
      (p, if(filteredRDD.count > 0) rdd else emptyRDD)
    }

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
    if(!platformResults.exists(_ == null)) {
      val totalRevenue = platformResults.foldLeft(0.0){(acc, element) =>
        acc + element.res
      }
      val results = new Results(totalRevenue, platformResults, lowerDate, upperDate)
      saveResultToDatabase(ThorContext.URI, outputCollection, results, companyName, applicationName)
      promise.success()
    } else {
      log.error("Count is zero")
      promise.failure(new Exception)
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
        onJobSuccess(companyName, applicationName, "Total Revenue", lowerDate, upperDate)
      } recover {
        case ex: Exception => {
          log.error("Job failed")
          onJobFailure(ex, "Total Revenue")
        }
      }
    }
    /** Must wait for all childs to finish **/
    case JobCompleted(jobType, status) => {
      childJobsCompleted = childJobsCompleted :+ jobType
      if(childJobsCompleted.size == dependants.size) {
        log.info("All child jobs have finished")
        supervisor ! JobCompleted(jobType, new Success)
        kill
      }
    }
  }
}
