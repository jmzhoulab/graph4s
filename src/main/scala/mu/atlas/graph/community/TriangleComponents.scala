package mu.atlas.graph.community

import org.apache.spark.graphx._
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2019/8/29.
  */
object TriangleComponents extends Serializable {

  type VertexSet = OpenHashSet[VertexId]

  /*
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], computeCC: Boolean = false): Graph[VertexId, ED] = {

    val initGraph = graph.mapVertices{case(vid, _) => immutable.Set(vid)}

    // 聚合邻居
    val vertexRDD = initGraph.aggregateMessages[immutable.Set[VertexId]](
      triplet => {
        triplet.sendToSrc(triplet.dstAttr)
        triplet.sendToDst(triplet.srcAttr)
      },
      (a, b) => a++b
    )

    val graphWithNeigs = initGraph.outerJoinVertices(vertexRDD)((vid, vdata, msg) => {
      msg.getOrElse(immutable.Set.empty[VertexId])
    })

    // 求相邻节点的公共邻居

    val triangleSubgraph = graphWithNeigs.mapTriplets(tr => {
      val isComNeigs = tr.srcAttr.intersect(tr.dstAttr).nonEmpty
      (tr.attr, isComNeigs)
    }).subgraph(epred = e => e.attr._2)
      .mapEdges(e => e.attr._1)

    if (computeCC)
      triangleSubgraph.connectedComponents()
    else
      triangleSubgraph.mapVertices{case(vid, _) => vid}
  }
  */

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], computeCC: Boolean = false): Graph[VertexId, ED] = {

    // Transform the edge data something cheap to shuffle and then canonicalize
    val canonicalGraph = graph.mapEdges(e => true)
      .subgraph(epred = e => e.srcId != e.dstId)

    // Get the triangle counts
    val counters = runPreCanonicalized(canonicalGraph).vertices

    // Join them bath with the original graph
    val triangleSubgraph = graph.outerJoinVertices(counters) { (vid, _, optCounter: Option[Int]) =>
      optCounter.getOrElse(0)
    }.subgraph(vpred = (vid, vdata) => vdata > 0)

    if (computeCC)
      triangleSubgraph.connectedComponents()
    else
      triangleSubgraph.mapVertices{case(vid, _) => vid}

  }

  private def runPreCanonicalized[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {
    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(nbrs.length)
        var i = 0
        while (i < nbrs.length) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }

    // join the sets with the graph
    val setGraph: Graph[VertexSet, ED] = graph.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.orNull
    }

    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(ctx: EdgeContext[VertexSet, ED, Int]) {
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext && counter == 0) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }

    // compute the intersection along edges
    val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _)
    // Merge counters with the graph and divide by two since each triangle is counted twice
    graph.outerJoinVertices(counters) { (_, _, optCounter: Option[Int]) =>
      optCounter.getOrElse(0)
    }
  }

}
