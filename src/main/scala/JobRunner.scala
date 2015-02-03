package wazza.thor

import akka.actor.ActorRef
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import java.util.Date
import org.quartz.Job
import org.quartz.JobDataMap
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
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

class JobRunner extends Job {

  lazy val system = ActorSystem("ThoR")
  lazy val sc = {
    val conf = new SparkConf()
      .setAppName("Wazza Analytics")
      .setMaster("local")
      .set("spark.scheduler.mode", "FAIR")
    new SparkContext(conf)
  }

  case class Apps(name: String, platforms: List[String])
  case class Company(name: String, apps: List[Apps])

  private def getCompanies = {
    val CompaniesCollectionName = "companiesData"
    val uri = new MongoClientURI(ThorContext.URI)
    val client = new MongoClient(uri)
    val collection = client.getDB(uri.getDatabase()).getCollection(CompaniesCollectionName)
    val companies = collection.find.toArray.asScala.map {obj =>
      val json = Json.parse(obj.toString)
      val companyName = (json \ "name").as[String]
      val apps = (json \ "apps").as[List[String]] map {app =>
        val collName = s"${companyName}_apps_${app}"
        val appCollection = client.getDB(uri.getDatabase()).getCollection(collName)
        val appInfo = Json.parse(appCollection.findOne.toString)
        new Apps(app, (appInfo \ "appType").as[List[String]])
      }
      new Company(companyName, apps)
    }
    client.close
    companies
  }

  protected def as[T](key: String)(implicit dataMap: JobDataMap): T = Option(dataMap.get(key)) match {
    case Some(item) =>
      // todo - more careful casting check?
      item.asInstanceOf[T]
    case None =>
      throw new NoSuchElementException("No entry in JobDataMap for required entry '%s'".format(key))
  }

  def execute(context: JobExecutionContext) = {
    try {
      implicit val dataMap = context.getJobDetail.getJobDataMap
      val thor = as[ActorRef]("ThorRef")
      val upper = as[Date]("upper")
      val lower = as[Date]("lower")
      println("JOB RUNNER starting at: " + context.getFireTime())
      println("LOWER: " + lower + " | UPPER: " + upper)
      for {
        c <- getCompanies
        app <- c.apps
      } {
        val supervisorName = s"${c.name}_supervisor_${app.name}".replace(' ','.')
        println(s"supervisor name: ${supervisorName}")
        system.actorOf(Supervisor.props(
          c.name,
          app.name,
          app.platforms,
          lower,
          upper,
          system,
          sc,
          thor
        ), name = supervisorName)
      }
    } catch {
      case ex: Exception => {
        println("ERROR: " + ex.getStackTraceString)
        val quartzException = new JobExecutionException(ex)
        throw quartzException
      }
    }
  }
}

