package wazza.thor

import scala.concurrent._
import ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, ActorRef}
import java.util.Date
import org.apache.spark._
import scala.collection.mutable.ListBuffer
import jobs._
import wazza.thor.messages._

class Supervisor(
  companyName: String,
  appName: String,
  start: Date,
  end: Date,
  system: ActorSystem,
  sc: SparkContext
) extends Actor with ActorLogging {

  private var jobs: List[ActorContext] = Nil
  private var results: List[JobCompleted] = Nil

  def initJobs() = {
    def generateName(name: String) = s"${companyName}_${name}_${appName}"
    var buffer = new ListBuffer[ActorContext]
    buffer += new ActorContext(system.actorOf(Props(new ActiveUsers(sc)), name = generateName("activeUsers")))
    buffer += new ActorContext(system.actorOf(Props(new NumberSessions(sc)), name = generateName("numberSessions")))
    buffer += new ActorContext(system.actorOf(Props(new NumberSessionsPerUser(sc)), name = generateName("numberSessionsPerUser")))
    buffer += new ActorContext(system.actorOf(Props(new PayingUsers(sc)), name = generateName("PayingUsers")))
    buffer += new ActorContext(system.actorOf(Props(new TotalRevenue(sc)), name = generateName("totalRevenue")))
    jobs = buffer.toList

    for(jobActor <- jobs) {
      jobActor.execute(companyName, appName, start, end)
    }
  }
  initJobs()

  def receive = {
    case JobCompleted(jobName, status) => {
      results = JobCompleted(jobName, status) :: results
      if(jobs.size == results.size) {
        //TODO save to DB
        context.stop(self)
      }
    }
  }
}

