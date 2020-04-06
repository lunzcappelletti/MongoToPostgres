package br.com.geru.dataOps.mongoToPostgres.application

import br.com.geru.dataOps.mongoToPostgres.security.Process

object Start {

  def main(args: Array[String]): Unit = {

    if (args(0) == "security") new Process() inputData()
    else if (args(0) == "ingestion") new Ingestion().execute()

  }

}
