package mu.atlas.graph.community

import org.junit._
import Assert._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph

/**
  * Created by zhoujiamu on 2019/8/28.
  */
@Test
class CliqueTest {

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

    val clique = Clique.run(graph)

    println("-"*30)
    clique.vertices.foreach(println)

    val res = clique.vertices.flatMap{case(vid, set) => set.map(cid => cid -> Seq(vid))}
      .reduceByKey(_++_).map(_._1).count()

    assertEquals(2, 2)

  }

}
