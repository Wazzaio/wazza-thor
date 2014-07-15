package wazza.io //TODO CHANGE TO io.wazza
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import io.wazza.jobs._
import org.apache.spark._
import scala.collection.mutable.ListBuffer

object JobRunner extends App {

  private var actors: List[ActorContext] = Nil

  private def setup = {
    val system = ActorSystem("analytics")
    val conf = new SparkConf().setAppName("Wazza Analytics").setMaster("local").set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)
    var buffer = new ListBuffer[ActorContext]
    buffer += new ActorContext(system.actorOf(Props(new SessionLength(sc)), name = "sessionLength"))
    buffer += new ActorContext(system.actorOf(Props(new TotalRevenue(sc)), name = "totalRevenue"))
    actors = buffer.toList
  }

  override def main(args: Array[String]): Unit = {

    val companies = List("CompanyTest")
    val apps = List("RecTestApp")
    setup
    for {
      c <- companies
      app <- apps //e funcao de companies
      
    } {
      println(c)
      println(app)
      println(actors)
      for(actor <- actors) {
        actor.getCollections(c, app)
        actor.execute
      }
    }


    /**
		val system = ActorSystem("analytics")
    val conf = new SparkConf().setAppName("Wazza Analytics").setMaster("local").set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)
		val sessionLengthActor = system.actorOf(Props(new SessionLength(sc)), name = "sessionLength")
	  val inputCollection = "CompanyTest_mobileSessions_RecTestApp"
	  val outputCollection = "CompanyTest_SessionLength_RecTestApp"
	  sessionLengthActor ! (inputCollection, outputCollection)
    val i = "CompanyTest_purchases_RecTestApp"
    val o = "CompanyTest_TotalRevenue_RecTestApp"
    val totalRevenueActor = system.actorOf(Props(new TotalRevenue(sc)), name = "totalRevenue")
    totalRevenueActor ! (i, o)
      * */
	}
}
