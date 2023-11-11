class CompareSets {

  def jaccardSimilarity(set1: Set[Int], set2: Set[Int]): Double = {
    val intersection = set1.intersect(set2)
    val union = set1.union(set2)
    intersection.size.toDouble / union.size
  }

}