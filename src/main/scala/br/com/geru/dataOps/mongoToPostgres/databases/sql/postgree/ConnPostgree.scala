package br.com.geru.dataOps.mongoToPostgres.databases.sql.postgree

import java.sql
import java.sql.{DriverManager, SQLException}

import br.com.geru.dataOps.mongoToPostgres.config.Logs

class ConnPostgree() extends Logs {

  def getConnection(user:String, password:String, host:String, port:Int, database:String): Option[sql.Connection] = {

    try{
    val assembledUri = s"jdbc:postgresql://$host:$port/$database"
    Class.forName("org.postgresql.Driver");
    val conn = DriverManager.getConnection(assembledUri, user, password)
    log.info("Connection to Postgres succesfull")
    Some(conn)
    } catch {case e: SQLException => log.error(e); None}

  }

}
