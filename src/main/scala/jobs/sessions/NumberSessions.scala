package wazza.thor.jobs

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
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
import scala.collection.immutable.StringOps
import wazza.thor.messages._

object NumberSessions {

  def props(ctx: SparkContext, d: List[ActorRef]): Props = Props(new NumberSessions(ctx, d))
}

class NumberSessions(
  ctx: SparkContext,
  d: List[ActorRef]
) extends Actor with ActorLogging  with CoreJob {
  import context._

  dependants = d

  def inputCollectionType: String = "mobileSessions"
  def outputCollectionType: String = "numberSessions"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    avgSessionLength: Double,
    lowerDate: Date,
    upperDate: Date
  ) = {
    val uri  = new MongoClientURI(uriStr)
    val client = new MongoClient(uri)
    val collection = client.getDB(uri.getDatabase()).getCollection(collectionName)
    val result = new BasicDBObject
    result.put("totalSessions", avgSessionLength)
    result.put("lowerDate", lowerDate.getTime)
    result.put("upperDate", upperDate.getTime)
    collection.insert(result)
    client.close()
  }

  private def executeJob(
    inputCollection: String,
    outputCollection: String,
    lowerDate: Date,
    upperDate: Date
      
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
      val numberSessions = (mongoRDD.map(arg => {
        (arg._2.get("id"), 1)
      })).groupByKey().count()

      saveResultToDatabase(uri, outputCollection, numberSessions, lowerDate, upperDate)
      promise.success()
    } else {
      log.error("count is zero")
      promise.failure(new Exception())
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
        upperDate
      ) map {res =>
        log.info("Job completed successful")
        onJobSuccess(companyName, applicationName, "Number Sessions", lowerDate, upperDate)
      } recover {
        case ex: Exception => {
          log.error("Job failed")
          onJobFailure(ex, "Number Sessions")
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

