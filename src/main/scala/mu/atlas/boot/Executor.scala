package mu.atlas.boot

import mu.atlas.graph.utils.Props
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2019/8/31.
  */
trait Executor {

  def execute[N: ClassTag](edgeTuple: RDD[(N, N)], prop: Props = null): RDD[String]

  def buildGraph[N: ClassTag](edgeTuple: RDD[(N, N)]): Graph[N, Null] = {
    val vertexMap = edgeTuple.flatMap(n => Iterator(n._1, n._2))
      .distinct().zipWithUniqueId().cache()

    val edges = edgeTuple.join(vertexMap)
      .map{case(_, (dst, srcId)) => (dst, srcId)}
      .join(vertexMap)
      .map{case(_, (srcId, dstId)) => Edge(srcId, dstId, null)}

    val vertex = vertexMap.map(x => x._2 -> x._1)

    Graph(vertex, edges)

  }

}
