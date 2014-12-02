package wazza.thor.jobs

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
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

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "TotalRevenue"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    totalRevenue: Double,
    lowerDate: Date,
    upperDate: Date,
    companyName: String,
    applicationName: String
  ) = {
    val uri  = new MongoClientURI(uriStr)
    val client = new MongoClient(uri)
    val collection = client.getDB(uri.getDatabase()).getCollection(collectionName)
    val result = new BasicDBObject
    result.put("totalRevenue", totalRevenue)
    result.put("lowerDate", lowerDate.getTime)
    result.put("upperDate", upperDate.getTime)
    collection.insert(result)
    client.close()
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
    val inputUri = s"${ThorContext.URI}.${inputCollection}"
    val outputUri = s"${ThorContext.URI}.${outputCollection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.output.uri", outputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val mongoRDD = ctx.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ).filter(t => {

      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toDouble.toLong) } catch { case _: Throwable => None }
      }

      parseFloat(t._2.get("time").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(lowerDate) * upperDate.compareTo(startDate) >= 0
        }
        case _ => false
      }
    })

    if(mongoRDD.count() > 0) {
      val totalRevenue = mongoRDD.map(arg => {
        val price = arg._2.get("price").toString.toDouble
        price
      }).sum//reduce(_ + _)

      saveResultToDatabase(
        ThorContext.URI,
        outputCollection,
        totalRevenue,
        lowerDate,
        upperDate,
        companyName,
        applicationName
      )
      promise.success()
    } else  {
      /**
      val x = ctx.newAPIHadoopRDD(
        jobConfig,
        classOf[com.mongodb.hadoop.MongoInputFormat],
        classOf[Object],
        classOf[BSONObject]
      )

      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toLong) } catch { case _: Throwable => None }
      }

      log.info("ARSE")
      log.info(s"TOTAL REVENUE --- ${inputUri} ||  lowerDate ${lowerDate} upperDate ${upperDate}")
      val el = x.collect.toList.head
      log.info(el.toString)
      val d = el._2.get("time").toString.toDouble.toLong
      log.info(s"PURCHASE DATE ${(new Date(d)).toString}")
        * */

      log.error("Count is zero")
      promise.failure(new Exception)
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
