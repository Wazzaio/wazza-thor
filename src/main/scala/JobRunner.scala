package wazza.thor

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.quartz.Job
import org.quartz.JobExecutionContext
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

  private def runTestMode(companyName: String, applicationName: String): Unit = {
    ThorContext.URI = "mongodb://localhost:27017/dev"//"mongodb://wazza-db-dev.cloudapp.net:27017/dev"
    val end = new LocalDate()
    val start = end.minusDays(7)
    val days = 1//Days.daysBetween(start, end).getDays()+1
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
          system.actorOf(Supervisor.props(c, app, List("Android", "iOS"), currentDay.toDate, nextDay.toDate, system, sc) , name = supervisorName)
        }
      }
  }

  def execute(context: JobExecutionContext) = {
    println("JOB RUNNER starting at: " + context.getFireTime())
    val upper = new DateTime().withTimeAtStartOfDay
    val lower = upper.minusDays(1)
    for {
      c <- getCompanies
      app <- c.apps
    } {
      val supervisorName = s"${c.name}_supervisor_${app}".replace(' ','.')
      system.actorOf(Supervisor.props(
        c.name,
        app.name,
        app.platforms,
        lower.toDate,
        upper.toDate,
        system,
        sc
      ), name = supervisorName)
    }
  }
}

