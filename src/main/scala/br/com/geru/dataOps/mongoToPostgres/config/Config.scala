package br.com.geru.dataOps.mongoToPostgres.config

import com.typesafe.config.ConfigFactory

class Config extends Logs {


  val conf = ConfigFactory.load()


  //Source
  val userMongo: String = conf.getString("userMongo")
  val hostMongo: String = conf.getString("hostMongo")
  val portMongo: String = conf.getString("portMongo")
  val optionsMongo: String = conf.getString("optionsMongo")
  val projectMongo: String = conf.getString("projectMongo")
  val databaseMongo: String = conf.getString("databaseMongo")
  val collectionMongo: String = conf.getString("collectionMongo")
  //val pathCheckpoint: String = conf.getString("pathCheckpoint")
  val pathStructureFields: String = conf.getString("pathStructureFields")
  val fieldFilterDate: String = conf.getString("fieldFilterDate")
  //val checkpointFile = new Checkpoint(pathCheckpoint)
  val fieldDateControlPostegree: String = conf.getString("fieldDateControlPostegree")
  val pathTempFile: String = if (conf.getString("pathTempFile").last == "\\") conf.getString("pathTempFile") else conf.getString("pathTempFile") + "\\"
  val beginDatawithOutChekpoint: String = conf.getString("beginDatawithOutChekpoint")
  val utc: Int = conf.getString("utc").toInt
  //val beginDate: String = checkpointFile.readCheckpointFile()


  //Sync
  val userPostgres: String = conf.getString("userPostgres")
  val uriPostgres: String = conf.getString("uriPostgres")
  val portPostgres: Int = conf.getString("portPostgres").toInt
  val databasePostgres: String = conf.getString("databasePostgres")
  val tablePostgres: String = conf.getString("tablePostgres")
  val endDate: String = conf.getString("endDate")

  //Keys
  val security = new br.com.geru.dataOps.mongoToPostgres.security.Security()
  val securityProcess = new br.com.geru.dataOps.mongoToPostgres.security.Process()

  val pathKeyPostgres: String = conf.getString("pathKeyPostgres")
  protected val passwordPostgres: String = security.decrypt(securityProcess.reafile(pathKeyPostgres))

  val pathKeyMongoDB: String = conf.getString("pathKeyMongoDB")
  protected val passwordMongo: String = security.decrypt(securityProcess.reafile(pathKeyMongoDB))

  //Function that read the file fields.structure
  def estructureFile(pathStructureFields:String): Option[List[List[String]]] = {
    try {
      val fields: List[List[String]] = scala.io.Source.fromFile(pathStructureFields).getLines() //Read file
        .toList.map(i => if (i.contains("#")) List("") else i.replace(" ", "").split("=>").toList) //Exclude spaces and #
        .filter(j => j != List("")) //Exclude empty lines
      //fields.foreach(println)
      log.debug("fields.structure loaded...")
      Some(fields)
    } catch {case e: Exception => println(e); log.error("fields.structure not loaded...check if the file exists or is in correct format"); None}

  }

}
