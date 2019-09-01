package mu.atlas.graph.score

import org.apache.spark.graphx._
import mu.atlas.graph.log.LogFactory
import org.apache.spark.ml.ExpandVectors
import org.apache.spark.ml.linalg.{Vector, Vectors}
import breeze.linalg.{Vector => BV}

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2019/8/13.
  */
object PageRank {

  private val logger = LogFactory.getLogger(getClass)

  /**
    * Run PageRank for a fixed number of iterations returning a graph
    * with vertex attributes containing the PageRank and edge
    * attributes the normalized edge weight.
    *
    * @tparam VD the original vertex attribute (not used)
    * @tparam ED the original edge attribute (not used)
    *
    * @param graph the graph on which to compute PageRank
    * @param numIter the number of iterations of PageRank to run
    * @param resetProb the random reset probability (alpha)
    *
    * @return the graph containing with each vertex containing the PageRank and each edge
    *         containing the normalized weight.
    */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      numIter: Int,
                                      resetProb: Double = 0.15
                                     ): Graph[Double, Double] = {
    runWithOptions(graph, numIter, resetProb)
  }

  /**
    * Run PageRank for a fixed number of iterations returning a graph
    * with vertex attributes containing the PageRank and edge
    * attributes the normalized edge weight.
    *
    * @tparam VD the original vertex attribute (not used)
    * @tparam ED the original edge attribute (not used)
    *
    * @param graph the graph on which to compute PageRank
    * @param numIter the number of iterations of PageRank to run
    * @param resetProb the random reset probability (alpha)
    * @param srcId the source vertex for a Personalized Page Rank (optional)
    *
    * @return the graph containing with each vertex containing the PageRank and each edge
    *         containing the normalized weight.
    *
    */
  def runWithOptions[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                 numIter: Int, resetProb:
                                                 Double = 0.15,
                                                 srcId: Option[VertexId] = None
                                                ): Graph[Double, Double] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute resetProb.
    // When running personalized pagerank, only the source vertex
    // has an attribute resetProb. All others are set to 0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
      if (!(id != src && personalized)) resetProb else 0.0
    }

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId, id: VertexId) => resetProb * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => resetProb
      }

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logger.info(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    rankGraph
  }

  /**
    * Run Personalized PageRank for a fixed number of iterations, for a
    * set of starting nodes in parallel. Returns a graph with vertex attributes
    * containing the pagerank relative to all starting nodes (as a sparse vector) and
    * edge attributes the normalized edge weight
    *
    * @tparam VD The original vertex attribute (not used)
    * @tparam ED The original edge attribute (not used)
    *
    * @param graph The graph on which to compute personalized pagerank
    * @param numIter The number of iterations to run
    * @param resetProb The random reset probability
    * @param sources The list of sources to compute personalized pagerank from
    * @return the graph with vertex attributes
    *         containing the pagerank relative to all starting nodes (as a sparse vector) and
    *         edge attributes the normalized edge weight
    */
  def runParallelPersonalizedPageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                                  numIter: Int,
                                                                  resetProb: Double = 0.15,
                                                                  sources: Array[VertexId]): Graph[Vector, Double] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    require(sources.nonEmpty, s"The list of sources must be non-empty," +
      s" but got ${sources.mkString("[", ",", "]")}")

    // TODO if one sources vertex id is outside of the int range
    // we won't be able to store its activations in a sparse vector
    val zero = ExpandVectors.asBreeze(Vectors.sparse(sources.size, List()))
    val sourcesInitMap = sources.zipWithIndex.map { case (vid, i) =>
      val v = ExpandVectors.asBreeze(Vectors.sparse(sources.size, Array(i), Array(resetProb)))
      (vid, v)
    }.toMap
    val sc = graph.vertices.sparkContext
    val sourcesInitMapBC = sc.broadcast(sourcesInitMap)
    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each source vertex with attribute 1.0.
    var rankGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices { (vid, attr) =>
        if (sourcesInitMapBC.value contains vid) {
          sourcesInitMapBC.value(vid)
        } else {
          zero
        }
      }

    var i = 0
    while (i < numIter) {
      val prevRankGraph = rankGraph
      // Propagates the message along outbound edges
      // and adding start nodes back in with activation resetProb
      val rankUpdates = rankGraph.aggregateMessages[BV[Double]](
        ctx => ctx.sendToDst(ctx.srcAttr :* ctx.attr),
        (a : BV[Double], b : BV[Double]) => a :+ b, TripletFields.Src)

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (vid, oldRank, msgSum) =>
          val popActivations: BV[Double] = msgSum :* (1.0 - resetProb)
          val resetActivations = if (sourcesInitMapBC.value contains vid) {
            sourcesInitMapBC.value(vid)
          } else {
            zero
          }
          popActivations :+ resetActivations
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      logger.info(s"Parallel Personalized PageRank finished iteration $i.")

      i += 1
    }

    rankGraph.mapVertices { (vid, attr) =>
      ExpandVectors.fromBreeze(attr)
    }
  }

  /**
    * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
    * PageRank and edge attributes containing the normalized edge weight.
    *
    * @tparam VD the original vertex attribute (not used)
    * @tparam ED the original edge attribute (not used)
    *
    * @param graph the graph on which to compute PageRank
    * @param tol the tolerance allowed at convergence (smaller => more accurate).
    * @param resetProb the random reset probability (alpha)
    *
    * @return the graph containing with each vertex containing the PageRank and each edge
    *         containing the normalized weight.
    */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
                                                       graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    runUntilConvergenceWithOptions(graph, tol, resetProb)
  }

  /**
    * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
    * PageRank and edge attributes containing the normalized edge weight.
    *
    * @tparam VD the original vertex attribute (not used)
    * @tparam ED the original edge attribute (not used)
    *
    * @param graph the graph on which to compute PageRank
    * @param tol the tolerance allowed at convergence (smaller => more accurate).
    * @param resetProb the random reset probability (alpha)
    * @param srcId the source vertex for a Personalized Page Rank (optional)
    *
    * @return the graph containing with each vertex containing the PageRank and each edge
    *         containing the normalized weight.
    */
  def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
                                                                  graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
                                                                  srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
      if (id == src) (resetProb, Double.NegativeInfinity) else (0.0, 0.0)
    }
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
                                  msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      var teleport = oldPR
      val delta = if (src==id) 1.0 else 0.0
      teleport = oldPR*delta

      val newPR = teleport + (1.0 - resetProb) * msgSum
      val newDelta = if (lastDelta == Double.NegativeInfinity) newPR else newPR - oldPR
      (newPR, newDelta)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = if (personalized) 0.0 else resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp = if (personalized) {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }

    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  } // end of deltaPageRank

}
