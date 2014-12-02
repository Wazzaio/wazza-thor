package wazza.thor.jobs

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
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

class NumberSessionsPerUser(
  ctx: SparkContext,
  d: List[ActorRef]
) extends Actor with ActorLogging with CoreJob {
  import context._

  dependants = d

  def inputCollectionType: String = "mobileSessions"
  def outputCollectionType: String = "numberSessionsPerUser"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    nrSessionsPerUser: List[(String,Int)],
    lowerDate: Date,
    upperDate: Date
  ) = {
    val promise = Promise[Unit]
    val lst = new java.util.ArrayList[BasicDBObject](nrSessionsPerUser map {el =>
      val obj = new BasicDBObject()
      obj.put("user",el._1.toString)
      obj.put("nrSessions", el._2.toInt)
      obj
    })
    Future {
      try {
        val uri  = new MongoClientURI(uriStr)
        val client = new MongoClient(uri)
        val collection = client.getDB(uri.getDatabase()).getCollection(collectionName)
        val result = new BasicDBObject
        result.put("nrSessionsPerUser", lst)
        result.put("lowerDate", lowerDate.getTime)
        result.put("upperDate", upperDate.getTime)
        collection.insert(result)
        client.close
        promise.success()
      } catch {
        case ex: Exception => {
          log.error("Error saving data - " + ex.getMessage)
          promise.failure(ex)
        }
      }
    }

    promise.future
  }

  private def executeJob(
    inputCollection: String,
    outputCollection: String,
    lowerDate: Date,
    upperDate: Date,
    companyName: String,
    applicationName: String
  ): Future[Unit] = {

    val promise = Promise[Unit]
    val uri = ThorContext.URI
    val inputUri = s"${uri}.${inputCollection}"
    val outputUri = s"${uri}.${outputCollection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.output.uri", outputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val mongoRDD = ctx.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ).filter((t: Tuple2[Object, BSONObject]) => {
      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toDouble.toLong) } catch { case _: Throwable => None }
      }

      parseFloat(t._2.get("startTime").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(lowerDate) * upperDate.compareTo(startDate) >= 0
        }
        case _ => false
       }
    })

    val count = mongoRDD.count()
    if(count > 0) {
      var b = false
      val numberSessionsPerUser = mongoRDD.map(arg => {
        (arg._2.get("userId").toString, 1)
      }).reduceByKey(_ + _).collect.toList

      val dbResult = saveResultToDatabase(ThorContext.URI,
        outputCollection,
        numberSessionsPerUser,
        lowerDate,
        upperDate
      )
      dbResult onComplete {
        case Success(_) => promise.success()
        case Failure(ex) => promise.failure(ex)
      }
    } else {
      log.error("count is zero")
      promise.failure(new Exception())
    }

    promise.future
  }

  def kill = stop(self)

  def receive = {
    case InitJob(companyName ,applicationName, lowerDate, upperDate) => {
      log.info(s"InitJob received - $companyName | $applicationName | $lowerDate | $upperDate")
      supervisor = sender
      executeJob(
        getCollectionInput(companyName, applicationName),
        getCollectionOutput(companyName, applicationName),
        lowerDate,
        upperDate,
        companyName,
        applicationName
      ) map {res =>
        log.info("Job completed successful")
        onJobSuccess(companyName, applicationName, "Number Sessions Per User", lowerDate, upperDate)
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
