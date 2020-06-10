package mu.atlas.graph.agg

import jiamz.shaded.guava.common.hash.{BloomFilter, Funnels}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId}
import breeze.linalg.Vector
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
  * Created by zhoujm on 2020/6/4.
  */
object KnnAccumulate {

  /**
    * 布隆过滤器集合，支持自动扩展。当图中节点度相差较大时，这就会导致有些节点的k紧邻数量较大，而有些会较小。
    * 如果所有节点都是用单个布隆过滤器，那么需要考虑k紧邻数量较大的节点过滤问题，需要定义容量较大的过滤器，
    * 这个时候对于k紧邻数量小的邻居，使用容量大的过滤器是一种浪费，大材小用。因此这里定义一个可自动扩展布隆
    * 过滤器集合，对于需要容量大的节点，自动增加多个容量小的过滤器来解决。
    *
    * @param expectedInsertions Int 单个布隆过滤器期望的容量大小
    * @param fpp Float 单个布隆过滤器的错误率
    */
  case class BloomFilters(expectedInsertions: Int, fpp: Float){
    private var _count = 0
    private val filters = ArrayBuffer(BloomFilter.create(Funnels.longFunnel, expectedInsertions, fpp))

    def add(value: Long): Unit ={
      _count += 1
      if (_count > expectedInsertions) {    // 当前布隆过滤器超过设定的容量时，新增新的布隆过滤器来进行扩容
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

  /**
    * 节点属性类
    *
    * @param khop kbop 邻居集合，最短路为k的邻居集
    * @param filter 过滤器
    * @param acc 累加结果
    */
  case class Attr(khop: Set[(VertexId, Vector[Double])],
                  filter: BloomFilters,
                  acc: Vector[Double])


  /**
    * Knn累加方法，提供向量属性图、深度，计算在指定深度范围内邻居属性的累加结果
    *
    * @param graph 向量属性图
    * @param k 深度
    * @tparam ED 边属性的类型
    * @return knn累加结果
    */
  def run[ED: ClassTag](graph: Graph[Vector[Double], ED],
                       k: Int,
                       filterSize: Int = 100000,
                       fpp: Float = 0.001f): Graph[Vector[Double], ED] ={
    // 初始化节点属性
    val initGraph = graph.mapVertices{case(vid, vector) => {
      val filter = BloomFilters(filterSize, fpp)
      filter.add(vid)
      Attr(Set(vid -> vector), filter, Vector.zeros[Double](vector.length))
    }}

    def diff(attr1: Attr, attr2: Attr): Set[(VertexId, Vector[Double])] ={
      attr1.khop.filter(m => !attr2.filter.mightContain(m._1))
    }

    // pregel迭代累积knn的属性值
    val pregelGraph = initGraph.pregel(initialMsg = Set.empty[(VertexId, Vector[Double])], maxIterations = k)(
      vprog = (vid, attr, msg) => {
        if (msg.isEmpty)
          attr
        else {
          var acc = attr.acc
          msg.foreach(m => {
            attr.filter.add(m._1)      // 布隆过滤新增节点，标记该节点已被访问
            acc += m._2                // 累加
          })
          attr.copy(khop = msg, acc = acc)
        }
      },
      sendMsg = et => {   // 如果当前节点khop邻居不在对端节点上，则将其发送给对方
        Iterator(
          et.dstId -> diff(et.srcAttr, et.dstAttr),
          et.srcId -> diff(et.dstAttr, et.srcAttr)
        ).filter(_._2.nonEmpty)
      },
      mergeMsg = (msg1, msg2) => msg1 ++ msg2
    )
    pregelGraph.mapVertices((_, attr) => attr.acc)
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
    val pregelGraph = run(graph, k=3, filterSize = 10)

    pregelGraph.vertices.collect().foreach(x => {
      println(x)
      println(ObjectSizeCalculator.getObjectSize(x))
      println(ObjectSizeCalculator.getObjectSize(x._1))
      println(ObjectSizeCalculator.getObjectSize(x._2))
    })

  }

}
