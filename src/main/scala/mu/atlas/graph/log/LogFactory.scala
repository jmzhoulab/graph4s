package mu.atlas.graph.log

/**
  * Created by zhoujiamu on 2019/6/27.
  */
object LogFactory {
  def getLogger(clazz: Class[_]): Log = new Log(clazz)
}
