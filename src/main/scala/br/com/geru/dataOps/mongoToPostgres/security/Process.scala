package br.com.geru.dataOps.mongoToPostgres.security

import java.io.{FileWriter, PrintWriter}
import java.util.Scanner

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Source}

class Process() {

  def reafile(path:String): String = {
    var readFileKey: BufferedSource = Source.fromFile(path) //Le arquivo do caminho específico
    val contentFileKey: String = readFileKey.mkString //Transforma conteudo em unica string
    readFileKey.close()
    contentFileKey
  }

  def inputData(): Unit = {
    val input = new Scanner(System.in).asScala
    print("password: ")
    val senha = input.next()
    print("path: ")
    val path = input.next()

    persistData(path, senha)
  }

  private def persistData(path: String, passowrd: String): Unit = {
    val file = new FileWriter(path)
    val WriterFile = new PrintWriter(file)

    val security = new Security()
    val passwordEncryoted = security.encrypt(passowrd)

    WriterFile.println(passwordEncryoted)
    file.close()
  }


}





