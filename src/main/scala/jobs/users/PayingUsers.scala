package wazza.thor.jobs

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
import com.mongodb.casbah.Imports._
import play.api.libs.json._

object PayingUsers {
  
  def props(ctx: SparkContext, d: List[ActorRef]): Props = Props(new PayingUsers(ctx, d))
}

case class PurchaseInfo(purchaseId: String, time: Float)
sealed case class UserPurchases(userId: String, purchases: List[PurchaseInfo], lowerDate: Float, upperDate: Float)

class PayingUsers(
  ctx: SparkContext,
  d: List[ActorRef]
) extends Actor with ActorLogging  with CoreJob {
  import context._

  dependants = d

  override def inputCollectionType: String = "purchases"
  override def outputCollectionType: String = "payingUsers"

  implicit def userPurchaseToBson(u: UserPurchases): DBObject = {
    MongoDBObject(
      "userId" -> u.userId,
      "purchases" -> (u.purchases map {purchaseInfoToBson(_)}),
      "lowerDate" -> u.lowerDate,
      "upperDate" -> u.upperDate
    )
  }

  implicit def purchaseInfoToBson(p: PurchaseInfo): MongoDBObject = {
    MongoDBObject(
      "purchaseId" -> p.purchaseId,
      "time" -> p.time
    )
  }

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    payingUsers: List[(String, List[PurchaseInfo])],
    lowerDate: Date,
    upperDate: Date
  ): Future[Unit] = {
    val promise = Promise[Unit]
    Future {
      val uri  = MongoClientURI(uriStr)
      val client = MongoClient(uri)
      try {
        val collection = client.getDB(uri.database.get)(collectionName)
        payingUsers foreach {element =>
          val pInfo = UserPurchases(element._1, element._2, lowerDate.getTime, upperDate.getTime)
          collection.insert(pInfo)
        }
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
       parseFloat(t._2.get("lowerDate").toString) match {
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
          PurchaseInfo(purchases._2.get("id").toString, purchases._2.get("time").toString.toFloat)
        )
      })).groupByKey.map(purchaseInfo => {
        (purchaseInfo._1, purchaseInfo._2.toList.sortWith(_.time < _.time))
      }).collect.toList

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
