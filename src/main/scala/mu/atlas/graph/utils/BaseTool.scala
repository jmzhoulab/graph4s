package mu.atlas.graph.utils

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.log4j.{LogManager, Logger}

import scala.collection.immutable

/**
  * Created by zhoujiamu on 2019/8/28.
  */
object BaseTool {

  def md5(s: String): String = {
    val bytes = java.security.MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8"))
    val hex_digest = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
    val rs = new StringBuffer()
    for (i <- 0.to(15)) {
      rs.append(hex_digest(bytes(i) >>> 4 & 0xf))
      rs.append(hex_digest(bytes(i) & 0xf))
    }
    rs.toString
  }

  def md5ToLong(md5: String): Long = BigInt(md5.substring(0, 16), 16).toLong

  val format: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def currentTime: String = format.format(new Date())

  def string2date(string: String): Date = format.parse(string)

  def date2string(date: Date): String = format.format(date)

  def asString(obj: Any): String = {
    if (obj != null & obj != None) obj.toString else ""
  }

  def crossTuple[T](obj: Iterable[T]): immutable.IndexedSeq[(T, T)] = {
    val len = obj.size
    val array = obj.toSeq
    (0 until len-1).flatMap(i => (i+1 until len).map(j => array(i) -> array(j)))
  }

}
