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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps

class NumberPayingUsers(ctx: SparkContext) extends Actor with ActorLogging with WazzaActor {

  override def inputCollectionType: String = "purchases"
  override def outputCollectionType: String = "numberPayingUsers"

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

     def parseFloat(d: String): Option[Long] = {
       try { Some(d.toLong) } catch { case _: Throwable => None }
     }

    val promise = Promise[Unit]
     val inputUri = s"${ThorContext.URI}.${inputCollection}"
     val outputUri = s"${ThorContext.URI}.${outputCollection}"
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
      parseFloat(t._2.get("time").toString) match {
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
      val payingUsers = (mongoRDD.map(arg => {
        (arg._2.get("userId"), 1)
      })).groupByKey().count()

      println(s"NUMBER OF PAYING USERS $payingUsers")
      saveResultToDatabase(ThorContext.URI, outputCollection, payingUsers.toInt, lowerDate, upperDate)
      promise.success()
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
