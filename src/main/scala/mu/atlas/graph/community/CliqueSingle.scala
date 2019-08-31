package mu.atlas.graph.community

import scala.collection.mutable

/**
  * 挖掘极大团
  * Created by zhoujiamu on 2019/8/28.
  */
object CliqueSingle {

  def run(data: Array[(Long, Long)], minSize: Int = 3): mutable.Set[mutable.Set[Long]] = {
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

    val edge = Array(
      1L -> 2L,
      1L -> 4L,
      2L -> 6L,
      2L -> 3L,
      2L -> 4L,
      2L -> 5L,
      3L -> 5L
    )

    run(edge).foreach(println)

  }

}
