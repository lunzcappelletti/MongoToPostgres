package br.com.geru.dataOps.mongoToPostgres.databases.sql.postgree

import java.text.SimpleDateFormat
import java.util.Date

class Transform {

  def intersec(line: List[Any], datatypes: List[String]) = { //Function convert the datatypes of the one List
    val r = line.indices.map(i => List(line(i), datatypes(i))).toList
    r
  }


  def convertDatatype(value:String, datatype:String) ={
    datatype match {
      /*
      case "String" => value.toString
      case "Char" => value.toString
      case "Boolean" => try { try{value.toBoolean} catch {case e: Exception => 0} } catch {case er: Exception => ""}
      case "Byte" => try { try{value.toString.toByte} catch {case e: Exception => 0} } catch {case er: Exception => ""}
      case "Short" => try { try{value.toString.toShort} catch {case e: Exception => 0} } catch {case er: Exception => ""}
      case "Int" => try { try{value.toString.toInt} catch {case e: Exception => 0} } catch {case er: Exception => ""}
      case "Float" =>  try { try{value.toString.toFloat} catch {case e: Exception => 0.0} } catch {case er: Exception => ""}
      case "Double" => try { try{value.toString.toDouble} catch {case e: Exception => 0.0} } catch {case er: Exception => ""}
      */
      case "Date" =>  try {  try{val sdf = new SimpleDateFormat("yyyy-MM-dd"); sdf.format(new Date(value.toString.toLong))} catch {case e: Exception =>val sdf = new SimpleDateFormat("yyyy-MM-dd"); sdf.parse(value.toString.replace("\"","") )} } catch {case er: Exception => "1900-01-01"}
      case "Timestamp" => try { {try{val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); sdf.format(new Date(value.toString.toLong))} catch {case e: Exception => val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); sdf.parse(value.toString.replace("\"","") )}} } catch {case er: Exception => "1900-01-01 00:00:00"}
      case _ => value.toString.replace("'","''")
    }

  }
}