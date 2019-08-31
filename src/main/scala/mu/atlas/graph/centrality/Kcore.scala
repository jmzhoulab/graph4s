package mu.atlas.graph.centrality

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2018/5/14.
  */
object Kcore {

  /**
    * 求指定k值的k-core子图，思路是从指定
    * @param graph 图G，该图输入时要求将度数小于kValue的节点过滤掉，即最小度数是kValue
    * @param kValue k值
    * @tparam ED 边属性
    * @return 返回同一结构的图，节点属性第一维度值等于kValue的节点是值为kValue的k-core节点
    */
  def singleKcore[ED: ClassTag](graph: Graph[(Int, Boolean), ED], kValue: Int): Graph[(Int, Boolean), ED] = {

    type Msg = (Int, Boolean)   // boolean值控制每个节点只发送一次消息

    def vprog(v: VertexId, vAttr: Msg, msg: Msg): Msg = {
      (vAttr._1 - msg._1, vAttr._2 || msg._2)
    }

    def sendMsg(tr: EdgeTriplet[Msg, ED]): Iterator[(VertexId, Msg)] = {
      if (tr.srcAttr._1 < tr.dstAttr._1 && !tr.srcAttr._2 && tr.srcAttr._1 <= kValue)
        Iterator(
          tr.dstId -> (1, false),
          tr.srcId -> (0, true)
        )
      else if (tr.srcAttr._1 > tr.dstAttr._1 && !tr.dstAttr._2 && tr.dstAttr._1 <= kValue)
        Iterator(
          tr.dstId -> (0, true),
          tr.srcId -> (1, false)
        )
      else
        Iterator.empty
    }

    def mergeMsg(msg1: Msg, msg2: Msg) = (msg1._1 + msg2._1, msg1._2 || msg2._2)

    val minDegree = graph.vertices.map(_._2._1).reduce((a, b) => math.min(a, b))

    graph.pregel((0, false))(vprog, sendMsg, mergeMsg)

  }

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("Kcore")

    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(Array(
      1L -> 2L,
      1L -> 3L,
      2L -> 3L,
      1L -> 4L,
      4L -> 5L
    ))

    val graph = Graph.fromEdgeTuples(rdd, (0, false))

    val degreeGraph = graph.joinVertices(graph.degrees){case(vid, _, d) => (d, false)}

    degreeGraph.vertices.collect().foreach(println)

    singleKcore(degreeGraph, 2).subgraph(vpred = (vid: VertexId, attr: (Int, Boolean)) => attr._1 == 2)
      .vertices.collect().foreach(println)

  }

}
