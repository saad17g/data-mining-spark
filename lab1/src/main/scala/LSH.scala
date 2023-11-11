class LSH(threshold: Double, numBands: Int, rowsPerBand: Int) {


  def findCandidatePairs(signatures: Seq[Array[Int]]): Set[(Int, Int)] = {
    var candidatePairs = Set[(Int, Int)]()

    val n = signatures(0).length
    val s = threshold

    for (i <- 0 until numBands) {

      val bucket = scala.collection.mutable.Map[Int, List[Int]]()

      for (idx <- signatures.indices) {
        val band = signatures(idx).slice(i * rowsPerBand, (i + 1) * rowsPerBand)
        val bandStr = band.map(_.toString).mkString(" ")
        val hashBand = bandStr.hashCode

        val list = bucket.getOrElse(hashBand, Nil)
        bucket(hashBand) = idx :: list
      }

      bucket.values.foreach {
        case v if v.size == 1 =>
        case v =>
          for (i <- 0 until v.size; j <- i + 1 until v.size) {
            candidatePairs += ((v(i), v(j)))
          }
      }
    }
    candidatePairs
  }
}