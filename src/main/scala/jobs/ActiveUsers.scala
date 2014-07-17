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

class ActiveUsers(ctx: SparkContext) extends Actor with ActorLogging with WazzaContext with WazzaActor {

  override def inputCollectionType: String = "mobileSessions"
  override def outputCollectionType: String = "SessionLength"

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
    result.put("payingUsers", payingUsers)
    result.put("lowerDate", lowerDate)
    result.put("upperDate", upperDate)
    collection.insert(result)
    client.close()
  }

  def executeJob(inputCollection: String, outputCollection: String): Future[Unit] = {
    val promise = Promise[Unit]
    val inputUri = s"${URI}.${inputCollection}"
    val outputUri = s"${URI}.${outputCollection}"
    val df = new SimpleDateFormat("yyyy/MM/dd")
    val beginDate = new Date //df.parse(inputData.tail.tail.head)
    val endDate = new Date //df.parse(inputData.last)

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
    )/**.filter((t: Tuple2[Object, BSONObject]) => {
       val date = t._2.get("time")
       date match {
       case d: Date => {
       d.compareTo(beginDate) * endDate.compareTo(d) >= 0
       }
       case _ => {
       println(s"error - date class is " + date.getClass)
       false
       }
       }
       })**/

    val count = mongoRDD.count()
    if(count > 0) {
      val payingUsers = (mongoRDD.map(arg => {
        (arg._2.get("userId"), 1)
      })).groupByKey().count()

      println(s"NUMBER OF PAYING USERS $payingUsers")
      saveResultToDatabase(URI, outputCollection, payingUsers.toInt, beginDate, endDate)
      promise.success()
    } else {
      println("count is zero")
      promise.failure(new Exception)
    }

    promise.future
  }

  def receive = {
    case (companyName: String, applicationName: String) => {
      executeJob(
        getCollectionInput(companyName, applicationName),
        getCollectionOutput(companyName, applicationName)
      ) map {res =>
        println("SUCCESS")
      }
    }
  }
}
