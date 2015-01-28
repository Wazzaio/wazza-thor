package wazza.thor

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import com.typesafe.config._
import play.api.libs.json.Json
import scalaj.http.HttpOptions
import scalaj.http.{Http, Token}

case class NotificationMessage(cause: String, msg: String)

class NotificationsActor(apiKey: String, endpoint: String) extends Actor with ActorLogging {
  import context._

  private def generateEndpoint(module: String, action: String) : String = {
		return endpoint + module + "/" + action + ".json"
	}

  private def sendEmail(subject: String, to: List[String], message: String): Unit = {
    val params = Json.obj(
      "key" -> apiKey,
      "message" -> Json.obj(
        "subject" -> subject,
        "text" -> message,
        "from_email" -> "no-reply@wazza.io",
        "from_name" -> "Wazza",
        "to" -> (to map {m => Json.obj("email" -> m, "type" -> "to")})
      )
    )
    
    Http(generateEndpoint("messages", "send")).postData(params.toString)
      .header("Content-type", "application/json")
      .header("Chartset", "UTF-8")
      .option(HttpOptions.readTimeout(10000))
      .execute
  }

  def receive = {
    case NotificationMessage(cause, msg) => sendEmail("SPARK ERROR", List("joao@wazza.io", "duarte@wazza.io"), msg)
    case _ => log.info("Received unknown message")
  }
}

object NotificationsActor {

  private case class MailCredentials(apiKey: String, endpoint: String)
  private object MailCredentials {
    def apply(ops: (Option[String], Option[String])): Option[MailCredentials] = {
      ops match {
        case _ if(ops._1.isDefined && ops._2.isDefined) => {
          Some(new MailCredentials(ops._1.get, ops._2.get))
        }
        case _ => None
      }
    }
  }

  private def parseConfig: Option[MailCredentials] = {
    def getConfigElement(config: Config, key: String): Option[String] = {
      try {
        Some(config.getString(key))
      } catch {
        case _: Throwable => None
      }
    }

    val config = ConfigFactory.load().getConfig("mandrill")
    MailCredentials((getConfigElement(config, "apiKey"), getConfigElement(config, "endpoint")))
  }

  def apply: NotificationsActor = {
    parseConfig match {
      case Some(config) => new NotificationsActor(config.apiKey, config.endpoint)
      case _ => throw new Exception("Error occurred while initializing Mail worker")
    }
  }

  def props: Props = Props(NotificationsActor.apply)

}
