package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    // Hash each query
    val signedQueries: RDD[(IndexedSeq[Int], List[String])] = lshIndex.hash(queries)
    // Retrieve set of titles with the same signatures
    lshIndex.lookup(signedQueries).map(f => (f._2, f._3))
  }
}
