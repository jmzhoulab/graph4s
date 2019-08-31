package mu.atlas.graph.utils

import java.io.{FileInputStream, InputStreamReader}

/**
  * Created by zhoujiamu on 2018/7/11.
  */
class Props(path: String*) extends Serializable{

  private val prop = new java.util.Properties
  println(s"Load config files: ${path.mkString(",")}.")
  path.foreach(p => load(p))

  def keys(): Array[String] = prop.keySet().toArray.map(k => k.toString)

  def toMap: Map[String, String] = this.keys().map(key => key -> this.getProperty(key)).toMap

  def load(propertiesPath: String): Unit ={
    val property = new java.util.Properties
    property.load(new InputStreamReader(new FileInputStream(propertiesPath), "utf-8"))
    property.keySet().toArray().map(k => k.toString)
      .foreach(k => {
        var value = property.getProperty(k)
        value = value.replace("\\#", "110%^@001").split("#")
          .slice(0, 1).mkString("").replace("110%^@001", "#").trim
        setProperty(k, value)
      })
  }

  def loadFromXML(xmlPath: String){
    val property = new java.util.Properties
    property.loadFromXML(new FileInputStream(xmlPath))
    property.keySet().toArray().map(k => k.toString)
      .foreach(k => setProperty(k, property.getProperty(k)))
  }


  def setProperty(args: Array[String]): Unit ={
    val iterator = args.toIterator
    while (iterator.hasNext){
      val arg = iterator.next()
      val list = arg.split("=")
      if (list.length > 1)
        setProperty(list.head, list.slice(1,list.length).mkString("="))
      else if (list.length == 1 && arg.endsWith("="))
        setProperty(list.head, "")
      else if (arg.startsWith("--")) {
        val key = arg.replaceFirst("--", "")
        val value = iterator.next()
        if (value.startsWith("--")) println(s""""$key" got a value "$value".""")
        setProperty(key, value)
      }
    }
  }

  def addProperty(prop: java.util.Properties): Unit = {
    val kv = prop.keySet().toArray.map(k => k.toString -> prop.getProperty(k.toString, ""))
    setProperty(kv: _*)
  }

  def setProperty(key: String, value: String): Unit = {
    prop.setProperty(key, value)
    println(s"setProperty: $key = $value")
  }

  def setProperty(keyValue: (String, String)*): Unit = {
    keyValue.foreach{case(key, value) => setProperty(key, value)}
  }

  def getProperty(key: String, defaultValue: String = null): String = {
    var value = prop.getProperty(key, defaultValue)
    require(value.isInstanceOf[String], s"""Attribute "$key" is not exist.""")
    var matchRes = "\\$\\{.*?\\}".r.findAllIn(value)
    while (matchRes.nonEmpty) {
      matchRes.foreach(x => {
        val par = x.replace("$", "").replace("{", "").replace("}", "")
        value = value.replace("${%s}".format(par), getProperty(par)).trim
      })
      matchRes = "\\$\\{.*?\\}".r.findAllIn(value)
    }
    value
  }

}

object Props extends Serializable{

  def apply(path: String*): Props = new Props(path: _*)

}

