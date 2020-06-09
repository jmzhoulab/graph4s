package mu.atlas.graph.agg

import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId}
import breeze.linalg.Vector
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
  * Created by zhoujm on 2020/6/4.
  */
object AggKhop {

  case class BloomFilters(expectedInsertions: Int, fpp: Float){
    private var _count = 0
    private val filters = ArrayBuffer(BloomFilter.create(Funnels.longFunnel, expectedInsertions, fpp))

    def add(value: Long): Unit ={
      _count += 1
      if (_count > expectedInsertions) {
        filters.append(BloomFilter.create(Funnels.longFunnel, expectedInsertions, fpp))
        _count = 0
      } else
        filters.last.put(value)
    }

    def mightContain(value: Long): Boolean ={
      for (f <- filters){
        if (f.mightContain(value))
          return true
      }
      false
    }

    def size: Int = filters.length
  }

  case class Attr(khop: Set[(VertexId, Vector[Double])],
                  filter: BloomFilters,
                  acc: Vector[Double])


  def run[T: ClassTag](graph: Graph[Vector[Double], T], depth: Int): Graph[Vector[Double], T] ={
    graph.mapVertices{case(vid, vector) => {
      val filter = BloomFilters(100000, 0.001f)
      filter.add(vid)
      Attr(Set(vid -> vector), filter, Vector.zeros[Double](vector.length))
    }}.pregel(initialMsg = Set.empty[(VertexId, Vector[Double])], maxIterations = depth)(
      vprog = (_, attr, msg) => {
        if (msg.isEmpty)
          attr
        else {
          var acc = attr.acc
          msg.foreach(m => {
            attr.filter.add(m._1)
            acc += m._2
          })
          attr.copy(khop = msg, acc = acc)
        }
      },
      sendMsg = et => {
        Iterator(
          et.dstId -> et.srcAttr.khop.filter(m => !et.dstAttr.filter.mightContain(m._1)),
          et.srcId -> et.dstAttr.khop.filter(m => !et.srcAttr.filter.mightContain(m._1))
        )
      },
      mergeMsg = (msg1, msg2) => {
        if (msg1.size > 100000)
          msg1
        else if (msg2.size > 100000)
          msg2
        else
          msg1 ++  msg2
      }
    ).mapVertices((_, attr) => attr.acc)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Kcore")

    val sc = new SparkContext(conf)

    sc.setLogLevel("error")


    val rdd = sc.makeRDD(Array(
      0L -> 1L,
      0L -> 2L,
      1L -> 3L,
      2L -> 3L,
      2L -> 4L,
      3L -> 4L,
      4L -> 5L,
      4L -> 6L,
      6L -> 7L,
      7L -> 8L
    ))

    val graph = Graph.fromEdgeTuples(rdd, Vector(1.0))
    val pregelGraph = run(graph, 3)

    pregelGraph.vertices.collect().foreach(x => {
      println(x)
      println(ObjectSizeCalculator.getObjectSize(x))
      println(ObjectSizeCalculator.getObjectSize(x._1))
      println(ObjectSizeCalculator.getObjectSize(x._2))
    })

  }

}
