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
import scala.util.{Success,Failure}
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.Iterable
import scala.collection.immutable.StringOps
import wazza.thor.messages._

object PayingUsers {
  
  def props(ctx: SparkContext, d: List[ActorRef]): Props = Props(new PayingUsers(ctx, d))
}

class PayingUsers(
  ctx: SparkContext,
  d: List[ActorRef]
) extends Actor with ActorLogging  with CoreJob {
  import context._

  dependants = d

  override def inputCollectionType: String = "purchases"
  override def outputCollectionType: String = "payingUsers"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    payingUsers: List[(String, Iterable[(String, Float)])],
    lowerDate: Date,
    upperDate: Date
  ): Future[Unit] = {
    val promise = Promise[Unit]

    Future {
      val uri  = new MongoClientURI(uriStr)
      val client = new MongoClient(uri)
      try {
        val collection = client.getDB(uri.getDatabase()).getCollection(collectionName)
        val payingUsersDB = new ArrayList[BasicDBObject](payingUsers map {(el: Tuple2[String, Iterable[Tuple2[String, Float]]]) =>
          val obj = new BasicDBObject
          obj.put("userId", el._1)
          obj.put("purchases", new ArrayList[BasicDBObject](el._2.toList map {(p: Tuple2[String, Float]) =>
            val pObject = new BasicDBObject
            pObject.put("purchaseID", p._1)
            pObject.put("purchaseTime", p._2)
            pObject
          }))

          obj
        })
        val result = new BasicDBObject()
          .append("payingUsers", payingUsersDB)
          .append("lowerDate", lowerDate.getTime)
          .append("upperDate", upperDate.getTime)

        collection.insert(result)
        client.close
        promise.success()
      } catch {
        case ex: Exception => {
          log.error(ex.getMessage)
          client.close
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
     )/**.filter((t: Tuple2[Object, BSONObject]) => {
       def parseFloat(d: String): Option[Long] = {
         try { Some(d.toLong) } catch { case _: Throwable => None }
       }
       parseFloat(t._2.get("time").toString) match {
         case Some(dbDate) => {
           val startDate = new Date(dbDate)
           startDate.compareTo(lowerDate) * upperDate.compareTo(startDate) >= 0
         }
         case _ => false
       }
     })**/

     if(mongoRDD.count > 0) {
      val payingUsers = (mongoRDD.map(purchases => {
        (
          purchases._2.get("userId").toString,
          (purchases._2.get("id").toString, purchases._2.get("time").toString.toFloat)
        )
      })).groupByKey.collect.toList

       log.info(payingUsers.toString)

       val dbResult = saveResultToDatabase(ThorContext.URI,
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
      log.error("Count is zero")
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
        onJobSuccess(companyName, applicationName, "Paying Users", lowerDate, upperDate)
      } recover {
        case ex: Exception => {
          log.error("Job failed")
          onJobFailure(ex, "Paying Users")
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
