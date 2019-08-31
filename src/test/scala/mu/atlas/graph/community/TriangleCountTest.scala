package mu.atlas.graph.community

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.TriangleCount
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Created by zhoujiamu on 2019/8/29.
  */
@Test
class TriangleCountTest {

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

    val graph = Graph.fromEdgeTuples(rdd, null)

    val triangle = TriangleCount.run(graph)

    println("-"*30)
    triangle.vertices.foreach(println)

    assertEquals(2, 2)

  }



}
