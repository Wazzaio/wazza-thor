/*
 * wazza-thor
 * https://github.com/Wazzaio/wazza-thor
 * Copyright (C) 2013-2015  Duarte Barbosa, João Vazão Vasques
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package wazza.thor

import akka.actor.ActorRef
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.event.slf4j.Logger
import com.typesafe.config.ConfigFactory
import java.util.Date
import org.quartz.InterruptableJob
import org.quartz.Job
import org.quartz.JobDataMap
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.UnableToInterruptJobException
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

class JobRunner extends Job with InterruptableJob {

  lazy val system = ActorSystem("ThoR", ConfigFactory.load())
  lazy val sc = {
    val conf = new SparkConf()
      .setAppName("Wazza Analytics")
      .setMaster("local")     
      //.set("spark.scheduler.mode", "FAIR")
      .set("spark.logConf", "true")
    new SparkContext(conf)
  }

  case class Apps(name: String, platforms: List[String], paymentSystems: List[Int])
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
        new Apps(
          app,
          (appInfo \ "appType").as[List[String]],
          (appInfo \ "paymentSystems").as[List[Int]]
        )
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

  // this method is called by the scheduler
  override def interrupt() {
    try {
      system.shutdown
      Thread.currentThread.destroy()
    } catch {
      case e: Exception => {
        val ex = new  UnableToInterruptJobException(e)
        throw ex
      }
    }
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
          app.paymentSystems,
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

