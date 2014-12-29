package wazza.thor.jobs

import akka.actor.{ActorRef}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import play.api.libs.json.Json
import com.mongodb.casbah.Imports._

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
        val t2 = parseDate(dateStr)
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

  protected case class PlatformResults(platform: String, res: Double)
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
