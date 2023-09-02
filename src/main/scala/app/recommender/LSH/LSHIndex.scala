package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)
  private var titles: RDD[(Int, String, List[String])] = data
  private var partitioner: HashPartitioner = new HashPartitioner(100)
  private var signatures: RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = null
  private var sig_status = false

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    // Hashing the presented list of keywords
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    val hashed: RDD[(IndexedSeq[Int], List[String])] = hash(titles.map(_._3))
    val titlesKey: RDD[(List[String], (Int, String, List[String]))] = titles.keyBy(_._3)
    // Joining the hashes created with the information about the titles using the list of genres (or keywords) column
    val combined:RDD[(List[String], ((IndexedSeq[Int], List[String]), (Int, String, List[String])))] = hashed.keyBy(_._2).join(titlesKey).distinct()
    signatures = combined.map(f => (f._2._1._1, f._2._2)).groupByKey().map(f => (f._1, f._2.toList)).partitionBy(partitioner).cache()
    sig_status = true
    signatures
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    // Variable to avoid re-computation of buckets
    if (sig_status == false)
      getBuckets()
    // Looking up titles with the same signature
    signatures.join(queries).map(f => {
      (f._1, f._2._2, f._2._1)
    })
  }
}
