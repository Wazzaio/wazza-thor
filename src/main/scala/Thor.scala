package wazza.thor

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import java.util.Date
import org.joda.time.DateTime
import org.joda.time.DurationFieldType
import org.joda.time.LocalDate
import org.quartz.CronScheduleBuilder
import org.quartz.JobBuilder
import org.quartz.JobDetail
import org.quartz.JobKey
import org.quartz.ScheduleBuilder
import org.quartz.TriggerBuilder
import org.quartz.core.jmx.JobDataMapSupport
import org.quartz.impl.StdSchedulerFactory
import scala.collection.JavaConverters._
import wazza.thor.messages.ThorMessage

class Thor(debug: Boolean, var dates: List[DateTime] = Nil) extends Actor with ActorLogging {

  def createJob(args: Map[String, AnyRef] = Map()): JobDetail = {
    val upper = args.get("CURRENT_DAY") match {
      case Some(date) => date.asInstanceOf[DateTime]
      case None => new DateTime().withTimeAtStartOfDay()
    }

    val lower = upper.minusDays(1)
    val defaultArgs = Map[String, AnyRef](
      "ThorRef" -> self,
      "upper" -> upper.toDate,
      "lower" -> lower.toDate
    )

    JobBuilder.newJob(classOf[JobRunner])
      .usingJobData(JobDataMapSupport.newJobDataMap((defaultArgs ++ args).asJava))
      .withIdentity("ThorJobRunner", "ThorGroup")
      .build
  }

  def createTrigger(job: JobDetail) = {
    if(debug) {
      TriggerBuilder.newTrigger()
        .withIdentity("DailyThorSchedule","ThorGroup")
        .startNow
        .forJob(job)
        .build
    } else {
      val schedule = CronScheduleBuilder.dailyAtHourAndMinute(1, 0)
      TriggerBuilder.newTrigger()
        .withIdentity("DailyThorSchedule","ThorGroup")
        .startNow
        .withSchedule(schedule)
        .forJob(job)
        .build
    }
  }

  val scheduler = new StdSchedulerFactory().getScheduler
  scheduler.start

  // define the job and tie it to the Job Runner class
  val job = createJob()

  // Create schedule to run JobRunner - every day at 1AM
  val trigger = createTrigger(job)
  
  scheduler.scheduleJob(job, trigger)

  def receive = {
    case m: ThorMessage => {
      log.info(s"Supervisor ${m.name} has ended")

      //Shuts down jobrunner actor system
      if(debug && !dates.isEmpty) {
        dates = dates.drop(1)
        if(!dates.isEmpty) {
          val args = Map("CURRENT_DAY" -> new DateTime(dates.head))
          val debugJob = createJob(args)
          val debugTrigger = createTrigger(debugJob)
          val jobKey = new JobKey("ThorJobRunner", "ThorGroup")
          scheduler.interrupt(jobKey)
          scheduler.deleteJob(jobKey)
          scheduler.scheduleJob(debugJob, debugTrigger)
        }
      }
    }
  }
}

object Thor extends App {
  private lazy val DEFAULT_DAYS = 7

  val debugFlag = if(args.size == 1) args.head.asInstanceOf[String] == "true" else false
  val first = new LocalDate(new Date).withDayOfMonth(1)
  val days = DEFAULT_DAYS
  val dates = if(debugFlag) {
    List.range(0, days) map {index =>
      first.plusDays(days).toDateTimeAtStartOfDay()
    }
  } else List()

  ActorSystem("ThorRoot").actorOf(Props(new Thor(debugFlag, dates)), "ThoR")
}

