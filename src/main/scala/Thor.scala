package wazza.thor

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import org.quartz.CronScheduleBuilder
import org.quartz.JobBuilder
import org.quartz.ScheduleBuilder
import org.quartz.TriggerBuilder
import org.quartz.impl.StdSchedulerFactory

class Thor extends Actor {
  val scheduler = new StdSchedulerFactory().getScheduler
  scheduler.start

  // define the job and tie it to the Job Runner class
  val job = JobBuilder.newJob(classOf[JobRunner])
    .withIdentity("ThorJobRunner", "ThorGroup")
    .build

  // Create schedule to run JobRunner - every day at 1AM
  val schedule = CronScheduleBuilder.dailyAtHourAndMinute(1, 0)
  val trigger = TriggerBuilder.newTrigger()
    .withIdentity("DailyThorSchedule","ThorGroup")
    .startNow
    .withSchedule(schedule)
    .forJob(job)
    .build
  
  scheduler.scheduleJob(job, trigger)

  def receive = {
    //TODO receive notification of supervisors end
    case _ => {}
  }
}

object Thor extends App {
  ActorSystem("ThorRoot").actorOf(Props[Thor], "ThoR")
}

