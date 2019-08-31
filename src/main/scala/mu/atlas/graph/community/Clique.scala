package mu.atlas.graph.community

import mu.atlas.graph.utils.BaseTool._
import org.apache.log4j.LogManager
import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * 挖掘极大团
  * Created by zhoujiamu on 2019/8/28.
  */
object Clique {

  private val log = LogManager.getLogger(getClass)

  def run[VD, ED: ClassTag](graph: Graph[VD, ED], minCliqueSize: Int = 3): Graph[Set[VertexId], ED] = {

    val initGraph = graph.mapVertices{case(vid, _) => Set(vid)}

    // 聚合邻居
    val vertexRDD = initGraph.aggregateMessages[Set[VertexId]](
      triplet => {
        triplet.sendToSrc(triplet.dstAttr)
        triplet.sendToDst(triplet.srcAttr)
      },
      (a, b) => a++b
    )

    log.debug(
      s"""
         |${"-"*30}
         |第一次聚合完成后的节点收到的消息, top100:
         |${vertexRDD.take(100).mkString("\n")}
      """.stripMargin)

    val graphWithNeigs = initGraph.joinVertices(vertexRDD)((vid, vdata, msg) => vdata++msg)

    log.debug(
      s"""
        |${"-"*30}
        |第一次消息处理完成后的顶点状态, top100:
        |${graphWithNeigs.vertices.take(100).mkString("\n")}
      """.stripMargin)

    // 求相邻节点的公共邻居，并把公共邻居放在边上

    graphWithNeigs.mapTriplets(tr => {
      val comNeigs = tr.srcAttr.intersect(tr.dstAttr)
        .filter(x => x != tr.srcId && x != tr.dstId)
      tr.copy(attr = comNeigs)
    })


    val vertexWithLabel = graphWithNeigs.aggregateMessages[Set[VertexId]](
      triplet => {
        val intersect = triplet.srcAttr.intersect(triplet.dstAttr).toSeq.sorted
        val msg = md5ToLong(md5(intersect.mkString("")))
        if (intersect.length >= minCliqueSize){
          val allCombine = (minCliqueSize to intersect.length).flatMap(k => {
            combine(intersect.toArray, k)
          })
          val msgs = allCombine.map(c => md5ToLong(md5(c.sorted.mkString("")))).toSet
          triplet.sendToSrc(msgs)
          triplet.sendToDst(msgs)
        }
      },
      (a, b) => a++b
    )


    log.debug(
      s"""
         |${"-"*30}
         |第二次聚合完成后的节点收到的消息, top100:
         |${vertexWithLabel.take(100).mkString("\n")}
      """.stripMargin)

    // 去掉小于指定数量的团
    val cliqueIds = vertexWithLabel.flatMap{case(vid, set) => set.map(cid => cid -> Seq(vid))}
      .reduceByKey(_++_).filter(_._2.length >= minCliqueSize)
      .flatMap{case(cid, seq) => seq.map(vid => vid -> Set(cid))}
      .reduceByKey(_++_)

    graphWithNeigs.outerJoinVertices(cliqueIds)((vid, vdata, msg) => msg.getOrElse(Set.empty[VertexId]))
      .subgraph(vpred = (vid, vdata) => vdata.nonEmpty)

  }

  private def combine[T: ClassTag](arr: Array[T], k: Int): Array[Array[T]] = {
    val finalResultBuffer = new ArrayBuffer[Array[T]]()

    def select(dataList: Array[T], dataIndex: Int, resultList: Array[T], resultIndex: Int){
      val resultLen = resultList.length
      val resultCount = resultIndex + 1
      val copyResult = resultList.slice(0, resultLen)
      if (resultCount > resultLen) { // 全部选择完时，输出组合结果
        finalResultBuffer += copyResult
        return
      }
      // 递归选择下一个
      for (i <- dataIndex until dataList.length + resultCount - resultLen) {
        resultList(resultIndex) = dataList(i)
        select(dataList, i + 1, resultList, resultIndex + 1)
      }
    }

    select(arr, 0, arr.slice(0, k), 0)

    finalResultBuffer.toArray

  }

  def runLocal(data: Array[(Long, Long)], minSize: Int = 3): mutable.Set[mutable.Set[Long]] = {
    val neigs = data.flatMap{case(from, to) => Seq(from -> to, to -> from)}.distinct
      .groupBy(_._1).mapValues(_.map(_._2).toSet)

    def findCliques(potentialClique: mutable.Set[Long],
                    candidates: mutable.Set[Long],
                    alreadyFound: mutable.Set[Long],
                    cliques: mutable.Set[mutable.Set[Long]]
                   ): Unit ={
      if (candidates.isEmpty && alreadyFound.isEmpty) {
        cliques.add(potentialClique)
      } else {
        val diffSet = candidates.diff(neigs(candidates.union(alreadyFound).last))   //轴
        diffSet.foreach(candidateVertex => {
          //每个点对应的N步长以内的点集合
          val neighbourVertices = neigs(candidateVertex)
          findCliques(potentialClique ++ mutable.Set(candidateVertex),
            candidates.intersect(neighbourVertices), //交集
            alreadyFound.intersect(neighbourVertices),cliques)
          alreadyFound += candidateVertex
          candidates.remove(candidateVertex)
        })
      }
    }

    val potentialClique = mutable.Set[Long]()
    val candidates = mutable.Set[Long]()

    neigs.keySet.foreach(x=>{
      candidates.add(x)
    })

    val alreadyFound = mutable.Set[Long]()
    val cliques = mutable.Set[mutable.Set[Long]]()

    val start: Long = System.currentTimeMillis()
    findCliques(potentialClique,candidates ,alreadyFound, cliques)

    println(System.currentTimeMillis()-start+"ms-------------")

    cliques.filter(_.size >= minSize)

  }

  def main(args: Array[String]): Unit = {

    val a = Array(1,2,3,4,5)

    combine(a, 3).foreach(x => println(x.toList))

  }

}
