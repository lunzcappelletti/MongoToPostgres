package br.com.geru.dataOps.mongoToPostgres.databases.noSQL.mongoDB

import com.mongodb.{MongoClient, MongoClientURI}
import br.com.geru.dataOps.mongoToPostgres.config.Logs
import sun.rmi.runtime.Log

class ConnMongoDB extends Logs {

  def getConnection(user:String,password:String,host:String,port:String, options:String,project:String): Option[MongoClient] ={

    try {
      val assembledUri = s"mongodb://$user:$password@$host:$port/$project?$options"
      val uri = new MongoClientURI(assembledUri)
      val conn: MongoClient = new MongoClient(uri)
      log.info("Connection to MongoDB succesfull...")
      Some(conn)
    } catch {case e:Exception => log.error("Error to connect to MongoDB...") ;println(e); None}
  }

}
