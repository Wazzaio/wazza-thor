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

  override def main(args: Array[String]): Unit = {
    val lower = new DateMidnight()
    val upper = lower.plusDays(1)
    val e = new LocalDate().minusDays(1)
    val s = e.minusDays(7)
    for {
      c <- List("DemoCompany")//getCompanies
      app <- List("Demo")//c.apps
    } {
      println(c)
      val supervisorName = s"${c}_supervisor_${app}".replace(' ','.') //s"${c.name}_supervisor_${app}".replace(' ','.')
      system.actorOf(Supervisor.props(/**c.name**/c, app, lower.toDate, upper.toDate, system, sc) , name = supervisorName)
    }
	}
}
