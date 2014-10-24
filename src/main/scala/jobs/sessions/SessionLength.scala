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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps

class SessionLength(ctx: SparkContext) extends Actor with ActorLogging  with WazzaActor {

  def inputCollectionType: String = "mobileSessions"
  def outputCollectionType: String = "SessionLength"

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
    result.put("averageSession", avgSessionLength)
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

    def parseFloat(d: String): Option[Long] = {
      try { Some(d.toLong) } catch { case _: Throwable => None }
    }

    val promise = Promise[Unit]
    val uri = ThorContext.URI
    val inputUri = s"${uri}.${inputCollection}"
    val outputUri = s"${uri}.${outputCollection}"
    val df = new SimpleDateFormat("yyyy/MM/dd")
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.output.uri", outputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val mongoDf = new SimpleDateFormat("yyyy-MM-dd")
    val mongoRDD = ctx.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ).filter((t: Tuple2[Object, BSONObject]) => {
      parseFloat(t._2.get("startTime").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(lowerDate) * upperDate.compareTo(startDate) >= 0
        }
        case _ => {
          println(s"ERROR")
          false
        }
      }
    })


    val count = mongoRDD.count()
    if(count > 0) {
      val averageSessionLength = (mongoRDD.map(arg => {
        arg._2.get("sessionLength").toString.toDouble
      }).reduce(_ + _)) / count

      saveResultToDatabase(uri, outputCollection, averageSessionLength, lowerDate, upperDate)
      println("average session length " + averageSessionLength)
      promise.success()
    } else {
      println("count is zero")
      promise.failure(new Exception())
    }

    promise.future
  }

  def receive = {
    case (
      companyName: String,
      applicationName: String,
      lowerDate: Date,
      upperDate: Date
    ) => {
      executeJob(
        getCollectionInput(companyName, applicationName),
        getCollectionOutput(companyName, applicationName),
        lowerDate,
        upperDate
      ) map {res =>
        println("SUCCESS")
      }  
    }
    case InputCollection => println("hey")
  }
}

