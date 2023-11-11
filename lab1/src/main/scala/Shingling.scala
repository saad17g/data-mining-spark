import scala.collection.mutable

class Shingling(k: Int) {
  def createShingles(document: String): Set[Int] = {
    // Initialize a set to store unique shingles
    val shingles = mutable.Set[Int]()

    // Create k-shingles from the document
    val words = document.split("\\s+")
    for (i <- 0 until words.length - k + 1) {
      val shingle = words.slice(i, i + k).mkString(" ")
      val shingleHash = shingle.hashCode // Compute a hash value for the shingle
      shingles += shingleHash
    }

    shingles.toSet // Convert the mutable set to an immutable set and return
  }
}