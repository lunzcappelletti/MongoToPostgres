package br.com.geru.dataOps.mongoToPostgres.application

import java.io.{BufferedInputStream, FileInputStream, FileWriter, InputStream}
import java.nio.charset.Charset
import java.sql.Connection

import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoCursor}
import br.com.geru.dataOps.mongoToPostgres.config.Config
import br.com.geru.dataOps.mongoToPostgres.databases.noSQL.mongoDB.{ConnMongoDB, Extract}
import org.bson.Document
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import br.com.geru.dataOps.mongoToPostgres.quash.Transform

import scala.reflect.io


class Ingestion extends Config {

  def execute(): Unit = {

    //Charset.forName("UTF-8").newEncoder()

    log.info("Process Started...")
    //Prepare structure (deleting content memory file, clear temp)
    io.File(pathTempFile+"memory.temp").toFile.delete()

    //Extract data from File "fields.structure"
    val fields: List[List[String]] = estructureFile(pathStructureFields).get

    //Connect to Postgree
    log.info("Trying connect to Postgres...")
    val connSync = new br.com.geru.dataOps.mongoToPostgres.databases.sql.postgree.ConnPostgree()
    val openConnectionPostgree: Connection = connSync.getConnection(userPostgres, passwordPostgres, uriPostgres, portPostgres, databasePostgres).get

    //Obtain the last Checkpoint
    val queryLastDate = s"select max($fieldDateControlPostegree) as max from $tablePostgres limit 1"
    val resultQueryLastDate = openConnectionPostgree.prepareStatement(queryLastDate).executeQuery()
    var beginDate = ""
    while (resultQueryLastDate.next()) {
      beginDate = try{
        if (
             resultQueryLastDate.getArray("max").toString.size < 10
        ) beginDatawithOutChekpoint
        else resultQueryLastDate.getArray("max").toString
      } catch {case e: Exception => beginDatawithOutChekpoint}
    }

      /* Conection to MongoDB */
      log.info("Trying connect to MongoDB...")
      val conn: ConnMongoDB = new br.com.geru.dataOps.mongoToPostgres.databases.noSQL.mongoDB.ConnMongoDB()
      val openConnectionMongoDB: MongoClient = conn.getConnection(userMongo, passwordMongo, hostMongo, portMongo, optionsMongo, projectMongo).get

      //Extract using Data Filter and especific Columns from "fields.structure"
      log.info("Extraction started...")
      val extract: Extract = new Extract()
      val dataCollection: MongoCollection[Document] = extract.getCollection(openConnectionMongoDB, databaseMongo, collectionMongo).get
      val dataFiltered: MongoCursor[Document] = extract.filter(dataCollection, fields, fieldFilterDate, beginDate, endDate).get

      log.info("Trying transform MongoDocument to Json...")
      var jsonMongo = List("{}")
      try {
        while (dataFiltered.hasNext) {
          jsonMongo = List(dataFiltered.next().toJson) ++ jsonMongo;
        }
      }
      catch {case e: Exception => log.error(e)}
      log.info("Transformation MongoDocument to Json finished...")

      log.info("Trying transform to relational data...")
      val transform = new Transform()
      val relationalExtraction: List[List[String]] = transform.extract(fields, jsonMongo.filter(i => i != "{}"))
      log.info("Relational transformation finished...")

      //relationalExtraction.foreach(println)

      log.info("Datatype Convertion transformation started...")
      val transformDatatypes = new br.com.geru.dataOps.mongoToPostgres.databases.sql.postgree.Transform()
      val result: List[List[List[Any]]] = relationalExtraction.map(i => transformDatatypes.intersec(i, fields.map(e => e(2))))
      val relationalDataConverted: List[List[Any]] = result.map(i => i.map(e => transformDatatypes.convertDatatype(e.head.toString, e(1).toString)))
      val data: List[String] = relationalDataConverted.map(l => l.mkString("|"))
      log.info("Datatype Convertion transformation finished...")

      log.info("total lines to ingestion: "+data.size)



      val file = new FileWriter(pathTempFile+"memory.temp")
      data.foreach(i => file.append(i +"\n").flush())
      file.flush()

     //Insert Data into postgree
     log.info("Trying ingest the data...")
     val is: InputStream = new BufferedInputStream(new FileInputStream(pathTempFile+"memory.temp"))
     val queryInsert = s"""COPY $tablePostgres(${fields.map(i => i.head).mkString(",")}) FROM STDIN WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)"""
     val baseConn = openConnectionPostgree.asInstanceOf[BaseConnection]
     val copyManager = new CopyManager((baseConn));
     try {
       copyManager.copyIn(queryInsert, is);
       log.info("Success while try ingest the data...")
     } catch {case e: Exception => log.error(e); log.error("Error while ingested data...")}

      //Finish connections
      log.info("Closing Connections...")
      is.close()

      openConnectionMongoDB.close()
      openConnectionPostgree.close()
      log.info("Connections Closed...")

      file.close()
      io.File(pathTempFile+"memory.temp").toFile.delete()

      log.info("Process finished...")
    }
  }

