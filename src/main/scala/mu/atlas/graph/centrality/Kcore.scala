package mu.atlas.graph.centrality

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import mu.atlas.graph.Direction
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2018/5/14.
  */
object Kcore {

  private def getDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], direction: Direction): VertexRDD[Int] = {
    direction match {
      case Direction.Either => graph.degrees
      case Direction.In => graph.inDegrees
      case Direction.Out => graph.outDegrees
    }
  }

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      direction: Direction = Direction.Either,
                                      maxK: Int
                                     ): RDD[(VertexId, Int)] = {
    var degree = getDegree(graph, direction)

    // 0-core
    var kcoreResult = graph.vertices.sparkContext.emptyRDD[(VertexId, Int)]
    var workGraph = graph.outerJoinVertices(degree){(vid, vd, deg) => deg.getOrElse(0)}
    val vertices = graph.numVertices
    var lastResultCount = kcoreResult.count()
    var kNum = 0

    while (kNum <= maxK){
      var isConverged = false
      while (!isConverged && lastResultCount < vertices){

        val preKcoreResult = kcoreResult
        kcoreResult = workGraph.vertices.filter(_._2 == kNum).union(kcoreResult).cache()
        println("total kcoreResult count: " + kcoreResult.count)

        preKcoreResult.unpersist(blocking = false)

        val preWorkGraph = workGraph
        val subGraph = workGraph.subgraph(vpred = (vid, deg) => deg >= kNum+1)
        degree = getDegree(subGraph, direction)
        workGraph = subGraph.outerJoinVertices(degree){(vid, vd, deg) => deg.getOrElse(0)}.cache()

        workGraph.numVertices
        workGraph.numEdges

        preWorkGraph.unpersist(blocking = false)

        isConverged = lastResultCount == kcoreResult.count
        lastResultCount = kcoreResult.count
      }
      kNum += 1
    }
    kcoreResult
  }

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("Kcore")

    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(Array(
      1L -> 2L,
      3L -> 1L,
      2L -> 3L,
      1L -> 4L,
      4L -> 5L
    ))

    val graph = Graph.fromEdgeTuples(rdd, (0, false))

    val degreeGraph = graph.joinVertices(graph.degrees){case(vid, _, d) => (d, false)}

    degreeGraph.vertices.collect().foreach(println)

    run(degreeGraph, Direction.In, 5).collect().foreach(println)

  }

}
