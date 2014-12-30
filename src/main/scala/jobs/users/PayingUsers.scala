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
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import scala.util.{Success,Failure}
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.Iterable
import scala.collection.immutable.StringOps
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import scala.collection.mutable._

object PayingUsers {
  
  def props(ctx: SparkContext, d: List[ActorRef]): Props = Props(new PayingUsers(ctx, d))
}

case class PlatformPurchases(platform: String, purchases: List[PurchaseInfo])

case class PurchaseInfo(
  purchaseId: String,
  val time: Date,
  platform: Option[String] = None
) extends Ordered[PurchaseInfo] {
  def compare(that: PurchaseInfo): Int = this.time.compareTo(that.time)
}

sealed case class UserPurchases(
  userId: String,
  totalPurchases: List[PurchaseInfo],
  purchasesPerPlatform: List[PlatformPurchases],
  lowerDate: Date, upperDate: Date
)

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
      "purchases" -> (u.totalPurchases map {purchaseInfoToBson(_)}),
      "purchasesPerPlatform" -> (u.purchasesPerPlatform map {platformPurchaseToBson(_)}),
      "lowerDate" -> u.lowerDate,
      "upperDate" -> u.upperDate
    )
  }

  implicit def platformPurchaseToBson(p: PlatformPurchases): MongoDBObject = {
    MongoDBObject(
      "platform" -> p.platform,
      "purchases" -> (p.purchases map {purchaseInfoToBson(_)})
    )
  }

  implicit def purchaseInfoToBson(p: PurchaseInfo): MongoDBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "purchaseId" -> p.purchaseId
    builder += "time" -> p.time
    p.platform match {
      case Some(platform) => builder += "platform" -> platform
      case None => {}
    }
    builder.result
  }

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    payingUsers: List[UserPurchases],
    lowerDate: Date,
    upperDate: Date
  ): Future[Unit] = {
    val promise = Promise[Unit]
    Future {
      val uri  = MongoClientURI(uriStr)
      val client = MongoClient(uri)
      try {
        val collection = client.getDB(uri.database.get)(collectionName)
        payingUsers foreach {collection.insert(_)}
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
     upperDate: Date,
     platforms: List[String]
   ): Future[Unit] = {

     val promise = Promise[Unit]
     val inputUri = s"${ThorContext.URI}.${inputCollection}"
     val outputUri = s"${ThorContext.URI}.${outputCollection}"
    
     val jobConfig = new Configuration
     jobConfig.set("mongo.input.uri", inputUri)
     jobConfig.set("mongo.output.uri", outputUri)
     jobConfig.set("mongo.input.split.create_input_splits", "false")

     val rdd = ctx.newAPIHadoopRDD(
       jobConfig,
       classOf[com.mongodb.hadoop.MongoInputFormat],
       classOf[Object],
       classOf[BSONObject]
     ).filter(t => {
       def parseDate(d: String): Option[Date] = {
         try {
           val Format = "E MMM dd HH:mm:ss Z yyyy"
           Some(new SimpleDateFormat(Format).parse(d))
         } catch {case _: Throwable => None }
       }

       val dateStr = t._2.get("time").toString
       parseDate(dateStr) match {
         case Some(startDate) => {
           startDate.compareTo(lowerDate) * upperDate.compareTo(startDate) >= 0
         }
         case _ => false
       }
     })

     if(rdd.count() > 0) {
       val payingUsers = rdd.map(purchases => {
         def parseDate(d: String): Option[Date] = {
           try {
             val Format = "E MMM dd HH:mm:ss Z yyyy"
             Some(new SimpleDateFormat(Format).parse(d))
           } catch {case _: Throwable => None }
         }

         val time = parseDate(purchases._2.get("time").toString)
         val platform = (Json.parse(purchases._2.get("device").toString) \ "osType").as[String]
         val info = new PurchaseInfo(purchases._2.get("id").toString, time.get, Some(platform))
         (purchases._2.get("userId").toString, info)
       }).groupByKey.map(purchaseInfo => {
         val userId = purchaseInfo._1
         val purchasesPerPlatform = platforms map {p =>
           val platformPurchases = purchaseInfo._2.filter(_.platform match {
             case Some(opt) => opt == p
             case _ => false
           }) map {pp =>
             new PurchaseInfo(pp.purchaseId, pp.time)
           }
           new PlatformPurchases(p, platformPurchases.toList)
         }

         new UserPurchases(userId, purchaseInfo._2.toList, purchasesPerPlatform, lowerDate, upperDate)
       })
       val dbResult = saveResultToDatabase(ThorContext.URI,
         outputCollection,
         payingUsers.collect.toList,
         lowerDate,
         upperDate
       )

       dbResult onComplete {
         case Success(_) => promise.success()
         case Failure(ex) => promise.failure(ex)
       }
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
        upperDate,
        platforms
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
