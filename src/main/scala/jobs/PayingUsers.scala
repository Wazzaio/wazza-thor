package io.wazza.jobs

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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.util.{Success,Failure}
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.Iterable
import scala.collection.immutable.StringOps

class PayingUsers(ctx: SparkContext) extends Actor with ActorLogging with WazzaContext with WazzaActor {

  override def inputCollectionType: String = "purchases"
  override def outputCollectionType: String = "payingUsers"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    payingUsers: List[(String, Iterable[String])],
    lowerDate: Date,
    upperDate: Date
  ): Future[Unit] = {
    val promise = Promise[Unit]

    Future {
      try {
        val uri  = new MongoClientURI(uriStr)
        val client = new MongoClient(uri)
        val collection = client.getDB(uri.getDatabase()).getCollection(collectionName)
        val payingUsersDB = new ArrayList[BasicDBObject](payingUsers map {(el: Tuple2[String, Iterable[String]]) =>
          val obj = new BasicDBObject
          obj.put("userId", el._1)
          obj.put("purchases", new ArrayList[String](el._2.toList))
          obj
        })
        val result = new BasicDBObject()
          .append("payingUsers", payingUsersDB)
          .append("lowerDate", lowerDate)
          .append("upperDate", upperDate)

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

   def executeJob(
     inputCollection: String,
     outputCollection: String,
     lowerDate: Date,
     upperDate: Date
   ): Future[Unit] = {
    val promise = Promise[Unit]
    val inputUri = s"${URI}.${inputCollection}"
    val outputUri = s"${URI}.${outputCollection}"
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
      t._2.get("time") match {
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


     if(mongoRDD.count > 0) {
      val payingUsers = (mongoRDD.map(purchases => {
        (purchases._2.get("userId").toString, purchases._2.get("id").toString)
      })).groupByKey.collect.toList

      val dbResult = saveResultToDatabase(URI,
        outputCollection,
        payingUsers,
        lowerDate,
        upperDate
      )

      dbResult onComplete {
        case Success(_) => promise.success()
        case Failure(ex) => promise.failure(ex)
      }
    } else {
      println("count is zero")
      promise.failure(new Exception)
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
  }
}
