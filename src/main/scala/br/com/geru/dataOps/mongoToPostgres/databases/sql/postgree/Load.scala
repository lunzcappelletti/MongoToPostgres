package br.com.geru.dataOps.mongoToPostgres.databases.sql.postgree

import java.sql.PreparedStatement

import br.com.geru.dataOps.mongoToPostgres.config.Logs

class Load extends Logs {

  def multipleIngestion(conn:java.sql.Connection, table:String, columns:List[String], values:List[List[String]]): Unit ={

    try {

      //values.foreach(i => println(s"insert into $table (${columns.mkString(",")} ) values (${i.mkString(",")});" ))

      log.info("prepare Statement")
      val sql: List[PreparedStatement] = values.map(i => conn.prepareStatement( s"insert into $table (${columns.mkString(",")} ) values (${i.mkString(",")}) " ))

      log.info("Ingest...")
      //sql.foreach(println)
      sql.foreach(i => try{ i.execute() } catch { case e: Exception => println(e)})
    log.info(sql.size + " lines ingested...")
    }  catch {case e: Exception => log.error("Error to Load files in postgres...");  log.error(e);
    }

  }

}
