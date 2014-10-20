package io.wazza.jobs

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
import scala.collection.mutable.ArrayBuffer
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import scala.collection.JavaConversions._
import scala.util.{Success,Failure}
import scala.collection.immutable.StringOps

class NumberSessionsPerUser(ctx: SparkContext) extends Actor with ActorLogging with WazzaContext with WazzaActor {

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
          println(ex)
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
    upperDate: Date
      
  ): Future[Unit] = {
    val promise = Promise[Unit]
    val uri = URI
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
      t._2.get("startTime") match {
        case dbDate: BasicDBObject => {
          val ops = new StringOps(dbDate.get("$date").toString)
          val startDate = new SimpleDateFormat("yyyy-MM-dd").parse(ops.take(ops.indexOf('T')))
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
      var b = false
      val numberSessionsPerUser = mongoRDD.map(arg => {
        (arg._2.get("userId").toString, 1)
      }).reduceByKey(_ + _).collect.toList

      val dbResult = saveResultToDatabase(URI,
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
