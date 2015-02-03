package wazza.thor.jobs

import com.typesafe.config._

object ThorContext {

  private val conf = ConfigFactory.load()
  var URI = conf.getString("mongo.uri")
}

