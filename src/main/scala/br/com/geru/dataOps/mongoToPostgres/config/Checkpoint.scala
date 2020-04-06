package br.com.geru.dataOps.mongoToPostgres.config

import java.io.{File, FileWriter}
import java.util.Scanner

class Checkpoint(pathFile:String) {

  val file = new File(pathFile)

  def createCheckpointFile(content:String) = {
    val createFile = new FileWriter(pathFile)
    createFile.write(content)
    createFile.close()
  }

  def updateCheckpointFile(content:String) = {
      if (file.exists()) {
        deleteCheckpointFile(); createCheckpointFile(content)
      } else {createCheckpointFile(content)}
  }

  def readCheckpointFile(): String = {
    try {
    val read = new Scanner(new File(pathFile))
    read.next().replace(" ", "")
  } catch {
      case e: Exception => createCheckpointFile("2020-01-01")
        val read = new Scanner(new File(pathFile))
        read.next().replace(" ", "")}
  }

  private def deleteCheckpointFile() ={
        file.delete()
  }

}
