package mu.atlas.graph.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._

/**
  * Created by zhoujiamu on 2019/8/28.
  */
@Test
class PropsTest {

  @Test
  def testRun() = {

    val prop = new Props("/Users/stargraph/workspace/self/spark-launcher/conf/launcher.properties")

    println(prop.isExist("lala"))

  }

}
