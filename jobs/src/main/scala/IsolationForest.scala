sealed trait ITree

case class ITreeBranch(left: ITree, right: ITree, split_column: Int, split_value: Double) extends ITree

case class ITreeLeaf(size: Long) extends ITree

case class IsolationForest(num_samples: Long, trees: Array[ITree]) {

  def predict(x: Array[Double]): Double = {
    val predictions = trees.map(s => pathLength(x, s, 0)).toList
    math.pow(2, -(predictions.sum / predictions.size) / cost(num_samples))
  }

  def cost(num_items: Long): Int =
    (2 * (math.log(num_items - 1) + 0.5772156649) - (2 * (num_items - 1) / num_items)).toInt

  @scala.annotation.tailrec
  final def pathLength(x: Array[Double], tree: ITree, path_length: Int): Double = {
    tree match {
      case ITreeLeaf(size) =>
        if (size > 1) path_length + cost(size)
        else path_length + 1

      case ITreeBranch(left, right, split_column, split_value) =>
        val sample_value = x(split_column)

        if (sample_value < split_value)
          pathLength(x, left, path_length + 1)
        else
          pathLength(x, right, path_length + 1)
    }
  }
}
  