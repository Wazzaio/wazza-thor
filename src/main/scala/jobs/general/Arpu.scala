package wazza.thor.jobs

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.typesafe.config.{Config, ConfigFactory}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import scala.util.Try
  import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import scala.collection.immutable.StringOps

class Arpu(ctx: SparkContext) extends Actor with ActorLogging with WazzaActor with ChildJob {

  def inputCollectionType: String = "purchases"
  def outputCollectionType: String = "Arpu"

  def execute() = {

  }

  def receive = {
    case c: CoreJobCompleted => {
      addDependency(c.name)
      if(dependenciesCompleted) {
        //execute
      }
    }
    case _ => {
      //invalid message (log it)
    }
  }
}
