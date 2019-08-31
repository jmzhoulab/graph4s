package mu.atlas.graph.community

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Created by zhoujiamu on 2019/8/29.
  */
@Test
class CliqueSingleTest {

  @Test
  def testRun() = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("mu.atlas").setLevel(Level.DEBUG)

    val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("data/graph/clique-graph").map(line => {
      val list = line.split(" ").map(_.trim)
      list(0).toLong -> list(1).toLong
    })

    val graph = GraphGenerators.logNormalGraph(sc, 1000)

    val graphEdge = graph.edges.map(e => e.srcId -> e.srcId).collect()

    println(graph.vertices.count() +  " " + graph.edges.count())

    graph.degrees.sortBy(_._2, ascending = false).take(50).foreach(println)

    val clique = CliqueSingle.run(graphEdge)

    println("-"*30)
    clique.foreach(println)

    val edge = (1L to 2000L).flatMap(x => {
      (x to x + 5).map(y => x -> y).filter(_ => math.random > 0.4)
    }).toArray

    val clique1 = CliqueSingle.run(edge)

    println("-"*30)
    clique1.foreach(println)

    assertEquals(2, 2)

  }


}
