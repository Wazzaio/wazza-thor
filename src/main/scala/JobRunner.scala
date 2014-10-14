package wazza.io //TODO CHANGE TO io.wazza
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import io.wazza.jobs._
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

object JobRunner extends App with WazzaContext {

  private var actors: List[ActorContext] = Nil

  case class Company(name: String, apps: List[String])

  private def getCompanies = {
    val CompaniesCollectionName = "companiesData"
    val uri = new MongoClientURI(URI)
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

  private def setup = {
    val system = ActorSystem("analytics")
    val conf = new SparkConf()
      .setAppName("Wazza Analytics")
      .setMaster("local")
      .set("spark.scheduler.mode", "FAIR")
      //TODO later .set("log4j.configuration", "/Users/Joao/Wazza/analytics/conf")
    val sc = new SparkContext(conf)
    var buffer = new ListBuffer[ActorContext]
    buffer += new ActorContext(system.actorOf(Props(new ActiveUsers(sc)), name = "activeUsers"))
    buffer += new ActorContext(system.actorOf(Props(new NumberPayingUsers(sc)), name = "nrPayingUsers"))
    buffer += new ActorContext(system.actorOf(Props(new NumberSessions(sc)), name = "numberSessions"))
    buffer += new ActorContext(system.actorOf(Props(new NumberSessionsPerUser(sc)), name = "numberSessionsPerUser"))
    buffer += new ActorContext(system.actorOf(Props(new PayingUsers(sc)), name = "PayingUsers"))
    buffer += new ActorContext(system.actorOf(Props(new SessionLength(sc)), name = "sessionLength"))
    buffer += new ActorContext(system.actorOf(Props(new TotalRevenue(sc)), name = "totalRevenue"))
    actors = buffer.toList
  }

  override def main(args: Array[String]): Unit = {
    setup
    val lower = new DateMidnight()
    val upper = lower.plusDays(1)

    val e = new LocalDate().minusDays(1)
    val s = e.minusDays(7)
    val days = Days.daysBetween(s, e).getDays()+1

//    new JsArray(List.range(0, days) map {i =>{


    for {
      c <- getCompanies
      app <- c.apps
      
    } {
      println(s"COMPANY $c -- APPLICATION $app")
      for(actor <- actors) {
        actor.execute(c.name, app, lower.toDate, upper.toDate)
      }
    }
	}
}
