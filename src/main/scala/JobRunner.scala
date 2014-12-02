package wazza.thor
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import wazza.thor.jobs._
import org.apache.spark._
import org.joda.time.DateMidnight
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import scala.collection.JavaConverters._
import play.api.libs.json._
import com.github.nscala_time.time.Imports._
import org.joda.time.Days
import org.joda.time.LocalDate
import org.joda.time.DurationFieldType
import org.joda.time.DateTime
import wazza.thor.messages._

object JobRunner extends App {

  lazy val system = ActorSystem("analytics")

  def initSpark(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("Wazza Analytics")
      .setMaster("local")
      .set("spark.scheduler.mode", "FAIR")
    new SparkContext(conf)
  }

  lazy val sc = initSpark

  case class Company(name: String, apps: List[String])

  private def getCompanies = {
    val CompaniesCollectionName = "companiesData"
    val uri = new MongoClientURI(ThorContext.URI)
    val client = new MongoClient(uri)
    val collection = client.getDB(uri.getDatabase()).getCollection(CompaniesCollectionName)
    collection.find.toArray.asScala.map {obj =>
      val json = Json.parse(obj.toString)
      new Company(
        (json \ "name").as[String],
        (json \ "apps").as[List[String]]
      )
    }
  }

  private def runTestMode(companyName: String, applicationName: String): Unit = {
    ThorContext.URI = "mongodb://wazza:1234@wazza-mongo-dev.cloudapp.net:27018/dev"
    val end = new LocalDate()
    val start = end.minusDays(7)
    val days = Days.daysBetween(start, end).getDays()+1
    List.range(0, days) foreach {index =>
      val currentDay = start.withFieldAdded(DurationFieldType.days(), index)
      val nextDay = currentDay.plusDays(1)
      println(s"CURRENT DAY $currentDay")
      for {
        c <- List(companyName)
        app <- List(applicationName)
      } {
        val dayId = currentDay.toString.replaceAll("-","")
        val supervisorName = s"${c}_supervisor_${app}_${dayId}".replace(' ','.')
        system.actorOf(Supervisor.props(c, app, currentDay.toDate, nextDay.toDate, system, sc) , name = supervisorName)
      }
    }
  }

  override def main(args: Array[String]): Unit = {
    def parseEncodedString(str: String): String = {
      str.replaceAll("%20", " ")
    }
    
    if(args.size == 2) {
      val companyName = args.head.replaceAll("%20", " ")
      val applicationName = args.last.replaceAll("%20", " ")
      runTestMode(companyName, applicationName)

    } else {
      val lower = new DateMidnight()
      val upper = lower.plusDays(1)
      for {
        c <- getCompanies
        app <- c.apps
      } {
        val supervisorName = s"${c.name}_supervisor_${app}".replace(' ','.')
        system.actorOf(Supervisor.props(c.name, app, lower.toDate, upper.toDate, system, sc) , name = supervisorName)
      }
    }
	}
}
