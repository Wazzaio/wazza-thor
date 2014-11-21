package wazza.thor.jobs

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import scala.collection.immutable.StringOps
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
    val uri  = new MongoClientURI(uriStr)
    val client = new MongoClient(uri)
    val collection = client.getDB(uri.getDatabase()).getCollection(collectionName)
    val result = new BasicDBObject
    result.put("activeUsers", payingUsers)
    result.put("lowerDate", lowerDate.getTime)
    result.put("upperDate", upperDate.getTime)
    collection.insert(result)
    client.close()
  }

  def executeJob(
    inputCollection: String,
    outputCollection: String,
    lowerDate: Date,
    upperDate: Date
  ): Future[Unit] = {

    val promise = Promise[Unit]
    val inputUri = s"${ThorContext.URI}.${inputCollection}"
    val outputUri = s"${ThorContext.URI}.${outputCollection}"
    val df = new SimpleDateFormat("yyyy/MM/dd")
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
        try { Some(d.toLong) } catch { case _: Throwable => None }
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
      val payingUsers = (mongoRDD.map(arg => {
        (arg._2.get("userId"), 1)
      })).groupByKey().count()

      saveResultToDatabase(ThorContext.URI, outputCollection, payingUsers.toInt, lowerDate, upperDate)
      promise.success()
    } else {
      log.error("count is zero")
      promise.failure(new Exception)
    }

    promise.future
  }

  def kill = stop(self)

  def receive = {
    case InitJob(companyName, applicationName, lowerDate, upperDate) => {
      log.info(s"InitJob received - $companyName | $applicationName | $lowerDate | $upperDate")
      supervisor = sender
      executeJob(
        getCollectionInput(companyName, applicationName),
        getCollectionOutput(companyName, applicationName),
        lowerDate,
        upperDate
      ) map {res =>
        log.info("Job completed successful")
        onJobSuccess(companyName, applicationName, "Active Users", lowerDate, upperDate)
      } recover {
        case ex: Exception => {
          log.error("Job failed")
          onJobFailure(ex, "Active Users")
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

