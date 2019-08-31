package mu.atlas.graph.log

import java.text.MessageFormat

import org.apache.log4j.LogManager
import org.apache.log4j.Logger

/**
  * Created by zhoujiamu on 2019/6/27.
  */
class Log extends Serializable {

  private var logger: Logger = null

  LogManager.getLogger(getClass)

  def this (clazz: Class[_] ) {
    this ()
    this.logger = LogManager.getLogger(clazz)
  }

  def warn (s: String): Unit = logger.warn (s)

  def warn (pattern: String, arguments: Any *): Unit = logger.warn (format (pattern, arguments) )

  def info (s: String): Unit = logger.info (s)
  def info (s: Any): Unit = logger.info (s.toString)

  def info (pattern: String, arguments: Any *): Unit = logger.info (format (pattern, arguments) )

  def error (e: Throwable): Unit = logger.error (e)

  def error (s: String, e: Throwable): Unit = logger.error (s, e)

  def error (pattern: String, arguments: Any *): Unit = logger.error (format (pattern, arguments) )

  def error (pattern: String, e: Throwable, arguments: Any *): Unit = logger.error (format (pattern, arguments), e)

  private def format (pattern: String, arguments: Any *): String = MessageFormat.format (pattern, arguments)
}



