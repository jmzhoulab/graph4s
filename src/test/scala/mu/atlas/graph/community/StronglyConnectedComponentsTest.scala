package mu.atlas.graph.community

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Created by zhoujiamu on 2019/8/28.
  */
@Test
class StronglyConnectedComponentsTest {

  @Test
  def testRun() = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("data/graph/clique-graph").map(line => {
      val list = line.split(" ").map(_.trim)
      list(0).toLong -> list(1).toLong
    })

    val graph = Graph.fromEdgeTuples(rdd, null)

    val cc = StronglyConnectedComponents.run(graph)

    println("-"*30)
    cc.vertices.foreach(println)
    assertEquals(2, 2)
  }

}
