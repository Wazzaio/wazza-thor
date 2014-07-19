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

class ItemRevenue(ctx: SparkContext) extends Actor with ActorLogging with WazzaContext with WazzaActor{

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "ItemRevenue"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    itemId: String,
    totalRevenue: Double,
    lowerDate: Date,
    upperDate: Date
  ) = {
    val uri  = new MongoClientURI(uriStr)
    val client = new MongoClient(uri)
    val collection = client.getDB(uri.getDatabase()).getCollection(collectionName)
    val result = new BasicDBObject
    result.put("itemRevenue", totalRevenue)
    result.put("itemId", itemId)
    result.put("lowerDate", lowerDate)
    result.put("upperDate", upperDate)
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
    )/**.filter((t: Tuple2[Object, BSONObject]) => {
       val purchaseDate = t._2.get("time")
       purchaseDate match {
       case d: Date => {
       d.compareTo(beginDate) * endDate.compareTo(d) >= 0
       }
       case _ => {
       println(s"error - date class is " + purchaseDate.getClass)
       false
       }
       }
       })**/

    if(mongoRDD.count() > 0) {
      val totalRevenue = mongoRDD.map(arg => {
        val price = arg._2.get("price").toString.toDouble
        price
      }).reduce(_ + _)

      saveResultToDatabase(URI, outputCollection, "", totalRevenue, lowerDate, upperDate)
      promise.success()
    } else  {
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
