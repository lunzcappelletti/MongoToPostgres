package br.com.geru.dataOps.mongoToPostgres.config

import org.apache.log4j.{Level, Logger}

trait Logs {

  val log = Logger.getLogger(this.getClass.getName)
  //Logger.getLogger("org").setLevel(Level.ERROR)


}
