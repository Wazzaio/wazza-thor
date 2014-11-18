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
import ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import org.joda.time.LocalDate

//TODO

object AvgTimeFirstPurchase {
  def props(sc: SparkContext): Props = Props(new AvgTimeFirstPurchase(sc))
}

class AvgTimeFirstPurchase(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  private lazy val Buyers = "Buyers" 

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
      "avgTimeBetweenPurchases" -> result,
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
    end: Date
  ): Future[Unit] = {
    val promise = Promise[Unit]

    def getBuyersCollection(uriStr: String): List[String] = {
      val collectionName = s"${companyName}_${Buyers}_${applicationName}"
      val uri  = MongoClientURI(uriStr)
      val client = MongoClient(uri)
      val collection = client.getDB(uri.database.get)(collectionName)
      val buyers = collection.findOne().get
      val result = (Json.parse(buyers.toString) \ "buyers").as[List[String]]
      client.close
      result
    }

    def getNumberSecondsBetweenDates(d1: Date, d2: Date): Float = {
      (new LocalDate(d2).toDateTimeAtCurrentTime.getMillis - new LocalDate(d1).toDateTimeAtCurrentTime().getMillis) / 1000
    }

    val inputUri = s"${ThorContext.URI}.${getCollectionInput(companyName, applicationName)}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    //TODO use subtract
    val firstTimeBuysRDD = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ).filter(element => {
      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toLong) } catch { case _: Throwable => None }
      }

      parseFloat(element._2.get("time").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(start) * end.compareTo(startDate) >= 0
        }
        case _ => false
      }
    })

    log.info("LEG")
    log.info(firstTimeBuysRDD.collect.toList.toString)

    val dateFields = ("lowerDate", "upperDate")
    val payingUsersCollName = s"${companyName}_payingUsers_${applicationName}"
    val uri = MongoClientURI(ThorContext.URI)
    val mongoClient = MongoClient(uri)
    val payingUsersCollection = mongoClient(uri.database.getOrElse("dev"))(payingUsersCollName)
    val query = (dateFields._1 $gte end.getTime $lte start.getTime) ++ (dateFields._2 $gte end.getTime $lte start.getTime)
    mongoClient.close
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper) => {
      log.info(s"core job ended ${sender.toString}")
      updateCompletedDependencies(sender)
      if(dependenciesCompleted) {
        log.info("execute job")
        executeJob(companyName, applicationName, upper, lower) map { arpu =>
          log.info("Job completed successful")
          onJobSuccess(companyName, applicationName, "Average Purchases Per Session")
        } recover {
          case ex: Exception => onJobFailure(ex, "Average Purchases Per Session")
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

