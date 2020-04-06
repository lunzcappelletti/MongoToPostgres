package br.com.geru.dataOps.mongoToPostgres.quash

import br.com.geru.dataOps.mongoToPostgres.config.Logs
import play.api.libs.json.{JsLookupResult, JsValue, Json}

class Transform extends Logs {


  def convertion(js:JsValue, s:String) ={
    val sp: Char = 46
    val path: List[String] = try{ s.split(sp).toList } catch {case e: Exception => println(e); List("")}

    var json = js
    path.foreach(js => {
      json = try{ json = json(js.toInt); json } catch {case e: Exception => try {json = json(js.toString); json } catch {case f: Exception => Json.parse("{}") }}
    })

    json
  }


  def extract(pathFields:List[List[String]], jsonList:List[String]): List[List[String]] = {
    try{
    val pf: List[String] = pathFields.map(i => i(1))
    val js: List[JsValue] = jsonList.map(i => try{Json.parse(i)} catch {case e: Exception => Json.parse("{}")})

    val matrix: List[List[String]] = js.map(json => pf.map(path => if (validate(convertion(json, path))=="") "" else  validate(convertion(json, path)) ))
    //matrix.foreach(println)
    matrix
    } catch {case e: Exception => log.error(e); List(List(""))}
  }


  def validate(j:JsValue): String = {
    try {
      (
        j.asOpt[String].getOrElse(j.asOpt[Long].getOrElse(j.asOpt[Boolean].get)
        )).toString
    } catch {
      case e: Exception => ""
    }
  }

}
