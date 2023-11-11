class CompareSignatures {

  def estimateSimilarity(sig1: Array[Int], sig2: Array[Int]): Double = {

    require(sig1.length == sig2.length, "Signatures must have same length")

    var agreements = 0
    val numHashes = sig1.length

    for (i <- 0 until numHashes) {
      if (sig1(i) == sig2(i)) {
        agreements += 1
      }
    }

    agreements.toDouble / numHashes

  }

}