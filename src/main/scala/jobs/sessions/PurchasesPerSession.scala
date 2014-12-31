package wazza.thor.jobs

import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import scala.collection.mutable.ListBuffer
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import scala.concurrent._
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps
import wazza.thor.messages._
import com.mongodb.casbah.Imports._
import play.api.libs.json._

object PurchasesPerSession {
  def props(sc: SparkContext): Props = Props(new PurchasesPerSession(sc))
}

sealed case class PurchasePerPlatform(platform: String, purchases: Int)
sealed case class PurchasePerUser(userId: String, totalPurchases: Int, platforms: List[PurchasePerPlatform])
object PurchasePerUser {
  def apply: PurchasePerUser = new PurchasePerUser(null, 0, List[PurchasePerPlatform]())
}

class PurchasesPerSession(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "payingUsers"
  def outputCollectionType: String = "PurchasesPerSession"

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: Double,
    end: Date,
    start: Date,
    companyName: String,
    applicationName: String
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val obj = MongoDBObject(
      "avgPurchasesSession" -> result,
      "lowerDate" -> start.getTime,
      "upperDate" -> end.getTime
    )
    collection.insert(obj)
    client.close
  }

  private def getNumberPurchases(
    inputCollection: String,
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): PurchasePerUser = {
    val inputUri = s"${ThorContext.URI}.${inputCollection}"
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val payingUsersRDD = sc.newAPIHadoopRDD(
      jobConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )/**.filter((t: Tuple2[Object, BSONObject]) => {
      def parseFloat(d: String): Option[Long] = {
        try { Some(d.toDouble.toLong) } catch { case _: Throwable => None }
      }
      parseFloat(t._2.get("lowerDate").toString) match {
        case Some(dbDate) => {
          val startDate = new Date(dbDate)
          startDate.compareTo(start) * end.compareTo(startDate) >= 0
        }
        case _ => false
      }
    })**/

    if(payingUsersRDD.count > 0) {
      val payingUsers = payingUsersRDD map {element =>
        val totalPurchases = Json.parse(element._2.get("purchases").toString).as[JsArray].value.size
        val purchasesPerPlatform = platforms map {platform =>
          val p = Json.parse(element._2.get("purchasesPerPlatform").toString).as[JsArray]
          val nrPurchases = p.value.filter(el => (el \ "platform").as[String] == platform).size
          new PurchasePerPlatform(platform, nrPurchases)
        }
        new PurchasePerUser(element._2.get("userId").toString, totalPurchases, purchasesPerPlatform)
      }
      val x = payingUsers.reduce{(res, current) => 
        val total = res.totalPurchases + current.totalPurchases
        val purchasesPerPlatforms: ListBuffer[PurchasePerPlatform] = new ListBuffer[PurchasePerPlatform]()
        platforms foreach {p =>
          println("platform: " + p)
          def purchaseCalculator(platforms: List[PurchasePerPlatform]) = {
            platforms.filter(_.platform == p).size
          }
          println("CURRENT: " + current.platforms + " | count: " + purchaseCalculator(current.platforms))
          println("ACCUM: " + res.platforms + " | count: " + purchaseCalculator(res.platforms))
          val updatedPurchases = purchaseCalculator(current.platforms) + purchaseCalculator(res.platforms)
          purchasesPerPlatforms += new PurchasePerPlatform(p, updatedPurchases)
        }
        println("TOTAL PURCHASES: " + purchasesPerPlatforms + "\n")
        new PurchasePerUser(null, total, purchasesPerPlatforms.toList)
      }
      println("RESULT: " + x)
      x
    } else {
      log.error("Count is zero")
      PurchasePerUser.apply
    }

  }

  def executeJob(
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): Future[Unit] = {
    val promise = Promise[Unit]

    println("PLATFORMS: " + platforms)

    val purchases = getNumberPurchases(
      getCollectionInput(companyName, applicationName),
      companyName,
      applicationName,
      start,
      end,
      platforms
    )

    println("PURCHASES!!")
    println(purchases)

    // val dateFields = ("lowerDate", "upperDate")
    // val nrSessionsCollName = s"${companyName}_numberSessions_${applicationName}"
    // val uri = MongoClientURI(ThorContext.URI)
    // val mongoClient = MongoClient(uri)
    // val nrSessionsCollection = mongoClient(uri.database.getOrElse("dev"))(nrSessionsCollName)
    // val query = (dateFields._1 $gte end.getTime $lte start.getTime) ++ (dateFields._2 $gte end.getTime $lte start.getTime)
    // val nrSessions = nrSessionsCollection.findOne(query).getOrElse(None)


    // nrSessions match {
    //   case None => {
    //     log.error("cannot find elements")
    //     promise.failure(new Exception)
    //   }
    //   case sessions => {
    //     val nrSessions = (Json.parse(sessions.toString) \ "totalSessions").as[Int]


    //     val result = if(nrSessions > 0 && nrPurchases != -1) nrPurchases / nrSessions else 0
    //     saveResultToDatabase(
    //       ThorContext.URI,
    //       getCollectionOutput(companyName, applicationName),
    //       result,
    //       start,
    //       end,
    //       companyName,
    //       applicationName
    //     )
    //     promise.success()
    //   }
    // }

    //mongoClient.close
    promise.future
  }

  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms) => {
      log.info(s"core job ended ${sender.toString}")
      updateCompletedDependencies(sender)
      if(dependenciesCompleted) {
        log.info("execute job")
        executeJob(companyName, applicationName, upper, lower, platforms) map { arpu =>
          log.info("Job completed successful")
          onJobSuccess(companyName, applicationName, "Average Purchases Per Session")
        } recover {
          case ex: Exception => onJobFailure(ex, "Average Purchases Per Session")
        }
      }
    }
    case CoreJobDependency(ref) => {
      log.info(s"Updating core dependencies: ${ref.toString}")
      addDependencies(ref)
    }
    case _ => log.debug("Received invalid message")
  }
}

