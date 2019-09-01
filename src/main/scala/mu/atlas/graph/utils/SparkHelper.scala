package mu.atlas.graph.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoujiamu on 2019/8/29.
  */
object SparkHelper {

  private val log: Logger = LogManager.getLogger(getClass)

  def clearPath(sc: SparkContext, outFile: String): Unit = {
    //若结果文件已存在，删除它
    val hadoopConf: Configuration = sc.hadoopConfiguration
    val hdfs: FileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new org.apache.hadoop.fs.Path(outFile)
    if (hdfs.exists(path)) {
      val result = hdfs.delete(path, true)
      if (result)
        log.info(s"""Path "$outFile" have been deleted successfully.""")
      else
        log.warn(s"""Path "$outFile" deleted field.""")
    }else
      log.info(s"""Path "$outFile" does not exist, don't need to delete.""")
  }

  def saveAsTextFile[T](outputData: RDD[T], outputFile: String): Unit ={
    val outputFileLocking: String = outputFile + "_LOCKING"      //加锁
    val sc = outputData.sparkContext
    clearPath(sc, outputFileLocking)  //若结果文件已存在，删除它

    outputData.saveAsTextFile(outputFileLocking)

    clearPath(sc, outputFile)
    HdfsHelper.rename(outputFileLocking, outputFile)

    log.info(s"Success to write to $outputFile")
  }

  def getSparkSession(appName: String, configs: Array[(String, String)] = null, enableHiveSupport: Boolean = false): SparkSession = {

    val builder = SparkSession.builder().appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")

    if (configs != null) configs.foreach{case(k, v) => builder.config(k, v)}

    val spark = if (enableHiveSupport) builder.enableHiveSupport().getOrCreate() else builder.getOrCreate()

    spark
  }

}
