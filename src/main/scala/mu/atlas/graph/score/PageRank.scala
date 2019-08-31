package mu.atlas.graph.score

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by zhoujiamu on 2019/8/13.
  */
object PageRank {

  private val log: Logger = LogManager.getLogger(getClass)

//  def run[VD, ED: ClassTag](graph: Graph[VD, ED], numIter: Int = Int.MaxValue): Graph[VertexId, ED] = {
//    require(numIter > 0, s"Number of iterations must be greater than 0," +
//      s" but got $numIter")
//
//  }

}
