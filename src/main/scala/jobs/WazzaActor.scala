package wazza.thor.jobs

import akka.actor.{ActorRef}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import play.api.libs.json.JsArray
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import com.mongodb.casbah.Imports._
import scala.collection.immutable.StringOps

trait WazzaActor {

  protected var supervisor: ActorRef = null

  def inputCollectionType: String
  def outputCollectionType: String
  def getCollectionInput(companyName: String, applicationName: String) =
    s"${companyName}_${inputCollectionType}_${applicationName}"

  def getCollectionOutput(companyName: String, applicationName: String) =
    s"${companyName}_${outputCollectionType}_${applicationName}"

  def getRDDPerPlatforms(
    timeField: String,
    platforms: List[String],
    rdd: RDD[(Object, BSONObject)],
    lowerDate: Date,
    upperDate: Date,
    ctx: SparkContext
  ): List[Tuple2[String, RDD[(Object, BSONObject)]]] = {
    val emptyRDD = ctx.emptyRDD[(Object, BSONObject)]
    platforms map {p =>
      val filteredRDD = rdd.filter(t => {
        def parseDate(d: String): Option[Date] = {
          try {
            val Format = "E MMM dd HH:mm:ss Z yyyy"
            Some(new SimpleDateFormat(Format).parse(d))
          } catch {case _: Throwable => None }
        }

        val dateStr = t._2.get(timeField).toString
        val time = parseDate(dateStr) match {
          case Some(startDate) => {
            startDate.compareTo(lowerDate) * upperDate.compareTo(startDate) >= 0
          }
          case _ => false
        }
        val platform = (Json.parse(t._2.get("device").toString) \ "osType").as[String] == p
        time && platform
      })
      (p, if(filteredRDD.count > 0) rdd else emptyRDD)
    }
  }

  protected def parseDate(json: JsValue, key: String): Date = {
    val dateStr = (json \ key \ "$date").as[String]
    val ops = new StringOps(dateStr)
    new SimpleDateFormat("yyyy-MM-dd").parse(ops.take(ops.indexOf('T')))
  }

  def getResultsByDateRange(
    collection: String,
    start: Date,
    end: Date
  ): Option[Results] = {
    val uri = MongoClientURI(ThorContext.URI)
    val mongoClient = MongoClient(uri)
    val dateFields = ("lowerDate", "upperDate")
    val coll = mongoClient(uri.database.getOrElse("dev"))(collection)
    val query = (dateFields._1 $gte end $lte start) ++ (dateFields._2 $gte end $lte start)
    coll.findOne(query) match {
      case Some(res) => {
        def parsePlatforms(arr: JsArray) = {
          arr.value map {(el: JsValue) =>
            new PlatformResults(
              (el \ "platform").as[String],
              (el \ "res").as[Double]
            )
          }
        }

        val jsonRes = Json.parse(res.toString)
        mongoClient.close()
        Some(new Results(
          (jsonRes \ "result").as[Double],
          parsePlatforms((jsonRes \ "platforms").as[JsArray]).toList,
          parseDate(jsonRes, "lowerDate"),
          parseDate(jsonRes, "upperDate")
        ))
      }
      case _ => {
        mongoClient.close()
        None
      }
    }
  }

  protected case class PlatformResults(platform: String, res: Double) extends Ordered[PlatformResults] {
    def compare(that: PlatformResults): Int = {
      this.platform.compareTo(that.platform)
    }
  }
  protected case class Results(
    result: Double,
    platforms: List[PlatformResults],
    lowerDate: Date,
    upperDate: Date
  )

  protected def resultsToMongo(results: Results): MongoDBObject = {
    MongoDBObject(
      "result" -> results.result,
      "platforms" -> (results.platforms map {p => MongoDBObject("platform" -> p.platform, "res" -> p.res)}),
      "lowerDate" -> results.lowerDate,
      "upperDate" -> results.upperDate
    )
  }

  protected def saveResultToDatabase(
    uriStr: String,
    collectionName: String,
    results: Results
  ) = {
    val uri  = MongoClientURI(uriStr)
    val client = MongoClient(uri)
    val collection = client.getDB(uri.database.get).getCollection(collectionName)
    collection.insert(resultsToMongo(results))
    client.close()
  }

  //Messages
  trait WazzaMessage
  case class InputCollection(companyName: String, applicationName: String) extends WazzaMessage
  case class OutputCollection(companyName: String, applicationName: String) extends WazzaMessage
}
