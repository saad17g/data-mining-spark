import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}

import scala.collection.mutable

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val conf = new SparkConf()
      .setAppName("lab1")
      .setMaster("local")
      .set("spark.testing.memory", "2147480000")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // Load the dataset from the CSV file
    val datasetPath = "data/Fake-News-Dataset.csv"
    val dataset: DataFrame = spark.read
      .option("header", "true")
      .csv(datasetPath)

    val numFunctions = 100
    val c = Int.MaxValue
    val hashFns = (1 to numFunctions).map { _ =>
      val a = util.Random.nextInt(Int.MaxValue)
      val b = util.Random.nextInt(Int.MaxValue)
      (x: Int) => ((a * x + b) % c).toInt
    }

    // Get document contents
    import spark.implicits._
    val documents = dataset.select("article_content").as[String].collect()

    val start = System.nanoTime()

    // Step 1 - Generate shingles
    val shingling = new Shingling(5)
    val shingles = documents.map(doc => shingling.createShingles(doc))

    // Step 2 - Jaccard similarity matrix
    val compareSets = new CompareSets()
    val jaccardMatrix = shingles.map { s1 =>
      shingles.map { s2 =>
        compareSets.jaccardSimilarity(s1, s2)
      }
    }

    // Step 3 - Generate minhash signatures
    val minHash = new MinHashing(hashFns, 100)
    val signatures = shingles.map(s => minHash.getMinHashSignature(s))

    // Step 4 - Signature similarity matrix
    val compareSigs = new CompareSignatures()
    val estimatedMatrix = signatures.map { sig1 =>
      signatures.map { sig2 =>
        compareSigs.estimateSimilarity(sig1, sig2)
      }
    }

    // Step 5 - Find candidate pairs
    val lsh = new LSH(0.95, 4, 25)
    val candidatePairs = lsh.findCandidatePairs(signatures)

    // Print candidate pairs
    candidatePairs.foreach { pair =>
      val similarity = compareSigs.estimateSimilarity(signatures(pair._1), signatures(pair._2))
      if (similarity > 0.95) {
        println(s"Similarity between ${pair._1} and ${pair._2} is $similarity")
      }
    }

    val end = System.nanoTime()
    val elapsed = end - start
    println(s"Execution time: ${elapsed/1e9} seconds")

    // Stop the Spark session
    spark.stop()
  }
}