package mu.atlas.graph.community

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Strongly connected components algorithm implementation.
  * Created by zhoujiamu on 2019/4/13.
  */
object StronglyConnectedComponents {

  private val log: Logger = LogManager.getLogger(getClass)

  /**
    * Compute the strongly connected component (SCC) of each vertex and return a graph with the
    * vertex value containing the lowest vertex id in the SCC containing that vertex.
    *
    * @tparam VD the vertex attribute type (discarded in the computation)
    * @tparam ED the edge attribute type (preserved in the computation)
    *
    * @param graph the graph for which to compute the SCC
    *
    * @return a graph with vertex attributes containing the smallest vertex id in each SCC
    */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int = 10): Graph[VertexId, ED] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got $numIter")

    // the graph we update with final SCC ids, and the graph we return at the end
    var sccGraph = graph.mapVertices { case (vid, _) => vid }.cache()

    // graph we are going to work with in our iterations
    var sccWorkGraph = graph.mapVertices{case(vid, _) => (0, 0, false)}.cache()

    var numVertices = sccWorkGraph.numVertices

    log.info("Total vertices in graph: " + numVertices)

    var iter = 0
    while (sccWorkGraph.numVertices > 0 && iter < numIter) {
      iter += 1
      numVertices = sccWorkGraph.numVertices

      log.info(s"Start iter $iter, remain not color vertices $numVertices")

      val outDegreeSccWorkGraph = sccWorkGraph.outerJoinVertices(sccWorkGraph.outDegrees) {
        (vid, data, degreeOpt) => (0, degreeOpt.getOrElse(0), false)
      }.cache()

      outDegreeSccWorkGraph.numVertices
      outDegreeSccWorkGraph.numEdges

      // currency vertex inDegrees, outDegrees, and isRemove
      val degreeSccWorkGraph =  outDegreeSccWorkGraph.outerJoinVertices(sccWorkGraph.inDegrees) {
        (vid, data, degreeOpt) => (degreeOpt.getOrElse(0), data._2, data._3)
      }.cache()

      degreeSccWorkGraph.numVertices
      degreeSccWorkGraph.numEdges

      log.info("Color vertices recursively according to degrees")

      val prevSccWorkGraph = degreeSccWorkGraph.pregel((0, 0, false))(
        (vid, myDegree, msgDegree) => {
          if (myDegree._3)
            myDegree
          else
            (myDegree._1 - msgDegree._1, myDegree._2 - msgDegree._2, myDegree._3 || msgDegree._3)
        },
        e => {
          if (e.srcAttr._1 == 0 && !e.srcAttr._3)
            Iterator(e.dstId -> (1, 0, false), e.srcId -> (0, 0, true))
          else if (e.dstAttr._2 == 0 && !e.dstAttr._3)
            Iterator(e.srcId -> (0, 1, false), e.dstId -> (0, 0, true))
          else
            Iterator.empty
        },
        (degree1, degree2) => (degree1._1 + degree2._1, degree1._2 + degree2._2, degree1._3 || degree2._3)
      )

      prevSccWorkGraph.numVertices
      prevSccWorkGraph.numEdges

      outDegreeSccWorkGraph.unpersist(blocking = false)
      degreeSccWorkGraph.unpersist(blocking = false)

      val finalVertices = prevSccWorkGraph.vertices.filter{case(vid, (curInDeg, curOutDeg, isFinal)) => isFinal}
        .mapValues{(vid, data) => vid}

      val prevSccGraph = sccGraph

      // write values to sccGraph
      sccGraph = sccGraph.outerJoinVertices(finalVertices) {
        (vid, scc, opt) => opt.getOrElse(scc)
      }.cache()

      sccGraph.numVertices
      sccGraph.numEdges
      prevSccGraph.unpersist(blocking = false)

      // only keep vertices that are not final
      val prevResult = sccWorkGraph
      sccWorkGraph = prevSccWorkGraph.subgraph(vpred = (vid, data) => !data._3)
        .partitionBy(RandomVertexCut)
        .cache()

      log.info("After color by degree, remain not color vertices " + sccWorkGraph.numVertices)
      sccWorkGraph.numEdges
      prevResult.unpersist(blocking = false)
      prevSccWorkGraph.unpersist(blocking = false)

      val resetSccWorkGraph = sccWorkGraph.mapVertices{case(vid, (_, _, isFinal)) => (vid, isFinal)}

      // collect min of all my neighbor's scc values, update if it's smaller than mine
      // then notify any neighbors with scc values larger than mine
      val pregelSccWorkGraph = Pregel[(VertexId, Boolean), ED, VertexId](
        resetSccWorkGraph, Long.MaxValue, activeDirection = EdgeDirection.Out)(
        (vid, myScc, neighborScc) => (math.min(myScc._1, neighborScc), myScc._2),
        e => {
          if (e.srcAttr._1 < e.dstAttr._1) {
            Iterator((e.dstId, e.srcAttr._1))
          } else {
            Iterator()
          }
        },
        (vid1, vid2) => math.min(vid1, vid2))

      pregelSccWorkGraph.edges.count()
      pregelSccWorkGraph.vertices.count()
      resetSccWorkGraph.unpersist(false)
      sccWorkGraph.unpersist(false)

      // start at root of SCCs. Traverse values in reverse, notify all my neighbors
      // do not propagate if colors do not match!

      val colorSccWorkGraph = Pregel[(VertexId, Boolean), ED, Boolean](
        pregelSccWorkGraph, false, activeDirection = EdgeDirection.In)(
        // vertex is final if it is the root of a color
        // or it has the same color as a neighbor that is final
        (vid, myScc, existsSameColorFinalNeighbor) => {
          val isColorRoot = vid == myScc._1
          (myScc._1, myScc._2 || isColorRoot || existsSameColorFinalNeighbor)
        },
        // activate neighbor if they are not final, you are, and you have the same color
        e => {
          val sameColor = e.dstAttr._1 == e.srcAttr._1
          val onlyDstIsFinal = e.dstAttr._2 && !e.srcAttr._2
          if (sameColor && onlyDstIsFinal) {
            Iterator((e.srcId, e.dstAttr._2))
          } else {
            Iterator()
          }
        },
        (final1, final2) => final1 || final2)

      val colorVertices = colorSccWorkGraph.vertices.filter{case(vid, (scc, isFinal)) => isFinal}.mapValues{(vid, data) => data._1}

      val prev1SccGraph = sccGraph

      // write values to sccGraph
      sccGraph = sccGraph.outerJoinVertices(colorVertices) {
        (vid, scc, opt) => opt.getOrElse(scc)
      }.cache()

      sccGraph.numVertices
      sccGraph.numEdges
      prev1SccGraph.unpersist(blocking = false)

      // only keep vertices that are not final
      val prev1Result = sccWorkGraph
      sccWorkGraph = colorSccWorkGraph.subgraph(vpred = (vid, data) => !data._2)
        .mapVertices{case(vid, _) => (0, 0, false)}.cache()

      sccWorkGraph.numVertices
      sccWorkGraph.numEdges
      prev1Result.unpersist(blocking = false)

      log.info(s"Finished iter $iter, remain not color vertices " + sccWorkGraph.numVertices)

      pregelSccWorkGraph.unpersist(blocking = false)
      colorSccWorkGraph.unpersist(blocking = false)

    }
    log.info("Finished scc")
    sccGraph
  }

}
