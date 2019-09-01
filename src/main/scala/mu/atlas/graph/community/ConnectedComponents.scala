package mu.atlas.graph.community

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2019/3/9.
  */
object ConnectedComponents {

  /**
    * Compute the connected component (CC) of each vertex and return a graph with the
    * vertex value containing the lowest vertex id in the CC containing that vertex.
    *
    * @tparam VD the vertex attribute type (discarded in the computation)
    * @tparam ED the edge attribute type (preserved in the computation)
    *
    * @param graph the graph for which to compute the SCC
    *
    * @return a graph with vertex attributes containing the smallest vertex id in each SCC
    */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int = Int.MaxValue): Graph[VertexId, ED] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got $numIter")

    val initGraph = graph.mapVertices{case(vid, _) => vid}
    val ccGraph = initGraph.pregel(Long.MaxValue, numIter)(
      vprog = (vid, vAttr, msg) => math.min(vAttr, msg),
      sendMsg = tr => {
        if (tr.srcAttr < tr.dstAttr)
          Iterator(tr.dstId -> tr.srcAttr)
        else if (tr.srcAttr > tr.dstAttr)
          Iterator(tr.srcId -> tr.dstAttr)
        else
          Iterator.empty
      },
      mergeMsg = (msg1, msg2) => math.min(msg1, msg2)
    )
    initGraph.unpersist()
    ccGraph

  }
}

