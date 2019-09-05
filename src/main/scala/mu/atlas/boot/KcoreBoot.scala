package mu.atlas.boot

import mu.atlas.graph.Direction
import mu.atlas.graph.centrality.Kcore
import mu.atlas.graph.utils.{BaseTool, Props}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2019/8/31.
  */
class KcoreBoot extends Executor {

  override def execute[N: ClassTag](edgeTuple: RDD[(N, N)], prop: Props = null): RDD[String] = {
    val start = BaseTool.getTimestamp
    val graph = buildGraph(edgeTuple)

    println("graph_vertex_count: " + graph.numVertices)
    println("graph_edge_count: " + graph.numEdges)

    val end = BaseTool.getTimestamp
    println("构图耗时: %.2fmin".format((end-start)/60000f))

    val direction = prop.getProperty("direction", "either").toLowerCase match {
      case "in" => Direction.In
      case "out" => Direction.In
      case _ => Direction.Either
    }

    val kNum = prop.getProperty("k", "5").toInt

    val kcore = Kcore.run(graph, direction, kNum)

    kcore.map{case(vid, _k) => s"$vid ${_k}"}

  }

}
