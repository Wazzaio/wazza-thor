package wazza.io //TODO CHANGE TO io.wazza
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import io.wazza.jobs._
import org.apache.spark._
import scala.collection.mutable.ListBuffer
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import scala.collection.JavaConverters._
import play.api.libs.json._

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
    val conf = new SparkConf().setAppName("Wazza Analytics").setMaster("local").set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)
    var buffer = new ListBuffer[ActorContext]
    buffer += new ActorContext(system.actorOf(Props(new SessionLength(sc)), name = "sessionLength"))
    buffer += new ActorContext(system.actorOf(Props(new TotalRevenue(sc)), name = "totalRevenue"))
    buffer += new ActorContext(system.actorOf(Props(new NumberPayingUsers(sc)), name = "payingUsers"))
    buffer += new ActorContext(system.actorOf(Props(new ActiveUsers(sc)), name = "activeUsers"))
    actors = buffer.toList
  }

  override def main(args: Array[String]): Unit = {
    setup
    for {
      c <- getCompanies
      app <- c.apps
      
    } {
      for(actor <- actors) {
        actor.execute(c.name, app)
      }
    }
	}
}
