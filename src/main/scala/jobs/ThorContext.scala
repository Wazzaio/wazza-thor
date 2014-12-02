package wazza.thor.jobs

import com.typesafe.config._

object ThorContext {

  private val conf = ConfigFactory.load()
  var URI = conf.getString("mongo.uri") //"mongodb://wazza:1234@wazza-mongo-dev.cloudapp.net:27018/dev"
}

