package org.apache.spark.ml

import org.apache.spark.ml.linalg.Vectors
import breeze.linalg.{Vector => BV}

/**
  * Created by zhoujiamu on 2019/8/31.
  */
object ExpandVectors {

  def fromBreeze(breezeVector: BV[Double]): linalg.Vector = Vectors.fromBreeze(breezeVector)

  def asBreeze(vec: linalg.Vector): BV[Double] = vec.asBreeze

}
