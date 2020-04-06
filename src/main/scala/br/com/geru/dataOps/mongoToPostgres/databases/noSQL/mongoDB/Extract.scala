package br.com.geru.dataOps.mongoToPostgres.databases.noSQL.mongoDB

import java.text.SimpleDateFormat
import java.time.Instant
import java.util
import java.util.Date

import com.mongodb.MongoClient
import com.mongodb.client.model.{Aggregates, Filters, Projections}
import com.mongodb.client.{AggregateIterable, MongoCollection, MongoCursor, MongoDatabase}
import br.com.geru.dataOps.mongoToPostgres.config.Config
import org.bson.Document
import org.bson.conversions.Bson

import scala.collection.JavaConverters._

class Extract() extends Config {

  def getCollection(conn: MongoClient, database:String, collection:String): Option[MongoCollection[Document]] ={

    try {
      val db = conn.getDatabase(database)
      val dataCollection: MongoCollection[Document] = db.getCollection(collection)
      Some(dataCollection)
    } catch { case e:Exception => log.error(e); None }
  }





    def fieldsAdjusted(fieldsToBeAdjusted: List[List[String]]) = {
      //fieldsToBeAdjusted.foreach(i => println(i))
      List("data_analise", "credit_analysis.decision.started_at")

        //println(fieldsToBeAdjusted)
        fieldsToBeAdjusted.map(i => if (!i(1).contains("$")) List(i(0), i(1)) else try{
          List(i(0), i(1).substring(0, i(1).lastIndexOf("$")  -1 ))
        } catch {case e: Exception =>
          List("", "")
          // List(i(0), try{i(1).toInt} catch {case e: Exception => i(1)}) //Force error with char $
        }
        ) // Mongo don't aceept $
      }


    def check(pathJson:String) = {


      val x = pathJson.split(".".toCharArray).toList
      val y = x.map(i => if ((try {
        i.toInt; 1
      } catch {
        case e: Exception => 0
      }) == 1) "" else i).filter(j => j != "")

      val z = y.map(i => if (i.contains("$")) { i.substring(0, i.lastIndexOf("$")  -1)} else {i})

      //println(z)
      y.mkString(".")
    }


  def filter(collection:MongoCollection[Document], fields:List[List[String]], fieldFilterDate:String, beginDate:String, endDate:String): Option[MongoCursor[Document]] = {
    try{

      val UTC = utc*3600000

      val date = beginDate.substring(0,10)
      val time = beginDate.substring(11,19)
      val beginDateTransformed = date + "T"+time+".999Z"


      val BeginIsoDate: Date = Date.from(Instant.parse(beginDateTransformed).plusMillis(UTC)) //Date.from(Instant.parse(filterDate+"T23:59:59.999Z"))
      val EndIsoDate: Date = Date.from(Instant.parse(endDate).plusMillis(UTC)) //Date.from(Instant.parse(endDate+"T23:59:59.999Z"))
      //val isoDate = Date.from(Instant.parse("2020-02-26T00:00:00.000Z"))

      log.info("Initial date: "+BeginIsoDate)
      log.info("last date: "+EndIsoDate)


      val lisFields: List[Bson] = fieldsAdjusted(fields).map(i => Projections.include(i.head, check(i(1)) ))
      //val a = lisFields.toIterator; while(a.hasNext) println(a.next().toString)

      val dataFiltered = collection.aggregate(util.Arrays.asList(
        //Aggregates.`match`(Filters.gt(fieldFilterDate, isoDate)), //>
        Aggregates.`match`(
          Filters.and(
            Filters.gt(fieldFilterDate, BeginIsoDate),
            Filters.lte(fieldFilterDate, EndIsoDate)
          )),
        //Aggregates.`match`(Filters.eq("credit_analysis.modules_data.loan.data.borrower.cpf", "000.226.415-30")),

        Aggregates.project(Projections.fields(lisFields:_*))
      )).iterator()

      //while (dataFiltered.hasNext) println(dataFiltered.next())
      log.info("Extraction from MongoDB succesfull...")
      //var sum = 0
      //while(dataFiltered.hasNext) sum = dataFiltered.next().size()+sum
      //log.info(sum+" MongoDB documents extracted...")

      Some(dataFiltered)
    } catch {case e:Exception => log.error(e);log.error("Error to extract data, check you MongoDB access...") ; None}

    }

}
