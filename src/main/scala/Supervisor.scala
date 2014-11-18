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
    log.info("Creating jobs")
    def generateName(name: String) = s"${companyName}_${name}_${appName}"
    var buffer = new ListBuffer[ActorRef]

    val ltv = generateActor(
      LifeTimeValue.props(sc),
      generateName("LTV")
    )

    val arpu = generateActor(
      Arpu.props(sc, ltv),
      generateName("Arpu")
    )

    val avgRevenuePerSession = generateActor(
      AverageRevenuePerSession.props(sc),
      generateName("avgRevenueSession")
    )

    val avgPurchasesSession = generateActor(
      PurchasesPerSession.props(sc),
      generateName("avgPurchasesSession")
    )

    val avgPurchasesUser = generateActor(
      AveragePurchasesUser.props(sc),
      generateName("avgPurchasesUser")
    )

    /**val avgTimeFirstPurchase = generateActor(
      AvgTimeFirstPurchase.props(sc),
      generateName("avgTimeFirstPurchases")
    )**/

    val avgTimeBetweenPurchases = generateActor(
      AvgTimeBetweenPurchases.props(sc),
      generateName("avgTimeBetweenPurchases")
    )

    val totalRevenue = generateActor(
      TotalRevenue.props(sc, List(arpu, avgRevenuePerSession)),
      generateName("totalRevenue")
    )
    val activeUsers = generateActor(
      ActiveUsers.props(sc, List(arpu)),
      generateName("activeUsers")
    )
    val payingUsers = generateActor(
      PayingUsers.props(sc, List(avgPurchasesSession, avgPurchasesUser, avgTimeBetweenPurchases/**, avgTimeFirstPurchase**/)),
      generateName("payingUsers")
    )
    val numberSessions = generateActor(
      NumberSessions.props(sc, List(avgRevenuePerSession, avgPurchasesSession)),
      generateName("numberSessions")
    )
    val nrSessionsPerUsers = generateActor(
      NumberSessionsPerUser.props(sc, List(ltv)),
      generateName("nrSessionsPerUser")
    )

    /** Define child's dependencies **/
    arpu ! CoreJobDependency(List(totalRevenue, activeUsers))
    avgRevenuePerSession ! CoreJobDependency(List(totalRevenue, numberSessions))
    avgPurchasesSession ! CoreJobDependency(List(payingUsers, numberSessions))
    avgPurchasesUser ! CoreJobDependency(List(payingUsers))
    avgTimeBetweenPurchases ! CoreJobDependency(List(payingUsers))
    ltv ! CoreJobDependency(List(arpu, nrSessionsPerUsers))
    //avgTimeFirstPurchase ! CoreJobDependency(List(payingUsers))

    buffer += totalRevenue
    buffer += activeUsers
    buffer += payingUsers
    buffer += numberSessions
    buffer += nrSessionsPerUsers
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
        log.info("All jobs have finished")
        stop(self)
      }
    }
    case _ => {
      log.info("DEAD LETTER")
      stop(self)
    }
  }
}

