package mu.atlas.boot

import mu.atlas.graph.community.TriangleComponents
import mu.atlas.graph.utils.{BaseTool, Props}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2019/8/31.
  */
class TriangleComponentsBoot extends Executor {

  override def execute[N: ClassTag](edgeTuple: RDD[(N, N)], prop: Props = null): RDD[String] = {
    val start = BaseTool.getTimestamp
    val graph = buildGraph(edgeTuple)

    println("graph_vertex_count: " + graph.numVertices)
    println("graph_edge_count: " + graph.numEdges)

    val end = BaseTool.getTimestamp
    println("构图耗时: %.2fmin".format((end-start)/60000f))

    val ccVertex = TriangleComponents.run(graph).vertices

    graph.vertices.join(ccVertex).map{case(vid, (raw, cid)) => s"$raw $cid"}

  }

}
