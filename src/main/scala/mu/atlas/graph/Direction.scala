package mu.atlas.graph

/**
  * Created by zhoujiamu on 2019/9/4.
  */
class Direction(private val name: String) extends Serializable {

  def reverse: Direction = this match {
    case Direction.In => Direction.Out
    case Direction.Out => Direction.In
    case Direction.Either => Direction.Either
  }

  override def equals(o: Any): Boolean = o match {
    case other: Direction => other.name == name
    case _ => false
  }

  override def hashCode: Int = name.hashCode

}

object Direction {
  /** Edges arriving at a vertex. */
  final val In: Direction = new Direction("In")

  /** Edges originating from a vertex. */
  final val Out: Direction = new Direction("Out")

  /** Edges originating from *or* arriving at a vertex of interest. */
  final val Either: Direction = new Direction("Either")
}
