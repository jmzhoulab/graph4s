package mu.atlas.boot

import mu.atlas.graph.utils._
import org.apache.log4j.{Level, LogManager}

/**
  * Created by zhoujiamu on 2019/8/31.
  */
object Algorithm {

  def main(args: Array[String]): Unit = {
    val start = BaseTool.getTimestamp
    val prop = new Props()
    prop.setProperty(args)

    if (prop.getProperty("silence", "false").toBoolean)
      LogManager.getLogger("org").setLevel(Level.WARN)

    val algName = prop.getProperty("alg.name")
    val input = prop.getProperty("input")
    val output = prop.getProperty("output")
    val fileFormat = prop.getProperty("file.format", "json")

    require(fileFormat == "json" || fileFormat == "csv", "参数'file.format'必须是json或者csv")

    val c = Class.forName(s"mu.atlas.boot.${algName}Boot")
    val obj = c.newInstance().asInstanceOf[Executor]

    val config = prop.keys().filter(_.startsWith("spark"))
      .map(k => k -> prop.getProperty(k))

    val spark = SparkHelper.getSparkSession(algName, config)

    val edgeTuple = spark.sparkContext.textFile(input).mapPartitions(iter => {
      if (fileFormat == "json")
        iter.map(line => {
          val jv = JsonHelper.parseJson(line)
          val from = JsonHelper.getJsonValue(jv, "_from")
          val to = JsonHelper.getJsonValue(jv, "_to")
          (from, to)
        })
      else {
        iter.map(line => {
          val list = line.split(",|\t")
          list(0).trim -> list(1).trim
        })
      }
    })

    println("edgeTuple_count: " + edgeTuple.count)

    val out = obj.execute(edgeTuple, prop)

    SparkHelper.saveAsTextFile(out, output)

    val end = BaseTool.getTimestamp

    println(s"${algName}算法运行耗时: %.2fmin".format((end-start)/60000f))

  }

}
