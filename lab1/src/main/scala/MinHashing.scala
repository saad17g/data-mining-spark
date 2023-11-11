class MinHashing(hashFunctions: Seq[Int => Int], numHashes: Int) {

  def getMinHashSignature(shingles: Set[Int]): Array[Int] = {

    val signature = new Array[Int](numHashes)

    hashFunctions.zipWithIndex.foreach {
      case (hashFn, index) =>
        var minHash = Int.MaxValue
        shingles.foreach { shingle =>
          val hashValue = hashFn(shingle)
          minHash = math.min(minHash, hashValue)
        }
        signature(index) = minHash
    }

    signature

  }

}