package wazza.thor.jobs


import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import org.apache.spark.rdd.RDD
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
import scala.collection.mutable.Map

object AveragePurchasesUser {
  def props(sc: SparkContext): Props = Props(new AveragePurchasesUser(sc))

  val platforms = List("Android", "iOS")

  def mapRDD(rdd: RDD[Tuple2[Object, BSONObject]]) = {
    rdd map {element =>
      val totalPurchases = Json.parse(element._2.get("purchases").toString).as[JsArray].value.size
      val purchasesPerPlatform = platforms map {platform =>
        val p = Json.parse(element._2.get("purchasesPerPlatform").toString).as[JsArray]
        val nrPurchases = p.value.find(el => (el \ "platform").as[String] == platform) match {
          case Some(platformPurchases) => (platformPurchases \ "purchases").as[JsArray].value.size
          case _ => 0
        }
        new PurchasePerPlatform(platform, nrPurchases)
      }
      new PurchasePerUser(element._2.get("userId").toString, totalPurchases, purchasesPerPlatform)
    }
  }

  def reduceRDD(rdd: RDD[PurchasePerUser]) = {
    rdd.reduce{(res, current) =>
      val total = res.totalPurchases + current.totalPurchases
      val purchasesPerPlatforms = platforms map {p =>
        def purchaseCalculator(pps: List[PurchasePerPlatform]) = {
          pps.find(_.platform == p).get.purchases
        }
        val updatedPurchases = purchaseCalculator(current.platforms) + purchaseCalculator(res.platforms)
        val result = new PurchasePerPlatform(p, updatedPurchases)
        result
      }
      new PurchasePerUser(null, total, purchasesPerPlatforms.toList)
    }
  }
}

sealed case class AveragePurchaseUserPerPlatform(platform: String, value: Double)
sealed case class AveragePurchaseUser(total: Double, platforms: List[AveragePurchaseUserPerPlatform])

class AveragePurchasesUser(sc: SparkContext) extends Actor with ActorLogging  with ChildJob {
  import context._

  def inputCollectionType: String = "payingUsers"
  def outputCollectionType: String = "avgPurchasesUser"

  private def averagePurchaseUserPerPlatformToBson(p: AveragePurchaseUserPerPlatform): MongoDBObject = {
    MongoDBObject("platform" -> p.platform, "res" -> p.value)
  }

  private def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    result: AveragePurchaseUser,
    start: Date,
    end: Date,
    companyName: String,
    applicationName: String,
    platforms: List[String]
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get)(collectionName)
    val platformResults = if(result.platforms.isEmpty) {
      platforms.map {p => MongoDBObject("platform" -> p, "res" -> 0.0)}
    } else {
      result.platforms.map{averagePurchaseUserPerPlatformToBson(_)}
    }
    val obj = MongoDBObject(
      "total" -> result.total,
      "platforms" -> platformResults,
      "lowerDate" -> start,
      "upperDate" -> end
    )
    collection.insert(obj)
    client.close
  }

  def executeJob(
    inputCollection: String,
    outputCollection: String,
    companyName: String,
    applicationName: String,
    start: Date,
    end: Date,
    platforms: List[String]
  ): Future[Unit] = {
    val promise = Promise[Unit]

    val inputUri = s"${ThorContext.URI}.${inputCollection}"    
    val jobConfig = new Configuration
    jobConfig.set("mongo.input.uri", inputUri)
    jobConfig.set("mongo.input.split.create_input_splits", "false")

    val payingUsersRDD = filterRDDByDateFields(
      ("lowerDate", "upperDate"),
      sc.newAPIHadoopRDD(
        jobConfig,
        classOf[com.mongodb.hadoop.MongoInputFormat],
        classOf[Object],
        classOf[BSONObject]
      ),
      start,
      end,
      sc
    )

    val result = if(payingUsersRDD.count > 0) {
      val payingUsers = AveragePurchasesUser.mapRDD(payingUsersRDD)
      val purchases = AveragePurchasesUser.reduceRDD(payingUsers)
      val nrUsers = payingUsers.count
      new AveragePurchaseUser(
        purchases.totalPurchases / nrUsers,
        purchases.platforms.map{p =>new AveragePurchaseUserPerPlatform(p.platform, p.purchases / nrUsers)}
      )
    } else {
      log.info("Count is zero")
      new AveragePurchaseUser(0.0, platforms map {new AveragePurchaseUserPerPlatform(_, 0.0)})
    }

    saveResultToDatabase(
      ThorContext.URI,
      getCollectionOutput(companyName, applicationName),
      result,
      start,
      end,
      companyName,
      applicationName,
      platforms
    )
    promise.success()
    promise.future
  }   
      
  def kill = stop(self)

  def receive = {
    case CoreJobCompleted(companyName, applicationName, name, lower, upper, platforms) => {
      log.info(s"core job ended ${sender.toString}")
      updateCompletedDependencies(sender)
      if(dependenciesCompleted) {
        log.info("execute job")
        executeJob(
          getCollectionInput(companyName, applicationName),
          getCollectionOutput(companyName, applicationName),
          companyName,
          applicationName,
          lower,
          upper,
          platforms
        ) map { arpu =>
          log.info("Job completed successful")
          onJobSuccess(companyName, applicationName, "Average Revenue Per Session")
        } recover {
          case ex: Exception => onJobFailure(ex, "Average Revenue Per Session")
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


