package wazza.thor

import scala.concurrent._
import ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import java.util.Date
import org.apache.spark._
import scala.collection.mutable.ListBuffer
import jobs._
import wazza.thor.messages._

object Supervisor {

  def props(companyName: String,
    appName: String,
    start: Date,
    end: Date,
    system: ActorSystem,
    sc: SparkContext
  ): Props = {
    Props(new Supervisor(companyName, appName, start, end, system, sc))
  }
}

class Supervisor(
  companyName: String,
  appName: String,
  start: Date,
  end: Date,
  system: ActorSystem,
  sc: SparkContext
) extends Actor with ActorLogging {
  import context._

  private var jobs = List[ActorRef]()
  private var results = List[JobCompleted]()

  private def generateActor(props: Props, name: String): ActorRef = {
    system.actorOf(props, name = name)
  }

  def initJobs() = {
    log.info("Creating core jobs")
    def generateName(name: String) = s"${companyName}_${name}_${appName}"
    var buffer = new ListBuffer[ActorRef]
    
    //buffer += generateActor(ActiveUsers.props(sc), generateName("activeUsers"))
    //buffer += generateActor(NumberSessions.props(sc), generateName("numberSessions"))
    //buffer += generateActor(NumberSessionsPerUser.props(sc), generateName("numberSessionsPerUser"))
    //buffer += generateActor(PayingUsers.props(sc), generateName("PayingUsers"))
    var arpu = generateActor(Arpu.props(sc), generateName("Arpu"))
    val totalRevenue = generateActor(TotalRevenue.props(sc, List(arpu)), generateName("totalRevenue"))
    buffer += totalRevenue
    jobs = buffer.toList
    
    for(jobActor <- jobs) {
      jobActor ! InitJob(companyName, appName, start, end)
    }
  }
  initJobs()

  def receive = {
    case JobCompleted(jobName, status) => {
      results = JobCompleted(jobName, status) :: results
      println(results)
      if(jobs.size == results.size) {
        //TODO save to DB
        stop(self)
      }
    }
    case _ => {
      log.info("DEAD LETTER")
      stop(self)
    }
  }
}

