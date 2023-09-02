package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = new HashPartitioner(20)

  // Variable that stores the sum of ratings as well the total number of ratings for a given movie
  private var aggRating: RDD[(Int, (Int, Double, Int))] = null
  private var titles: RDD[(Int, (Int, String, List[String]))] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    val movieRatings: RDD[(Int, (Int, Double, Int))] = ratings.map(f => (f._2, f._4, 1)).keyBy(_._1)
    // We add the sums of movies as well as the number of ratings
    aggRating = movieRatings.reduceByKey((x, y) => (x._1, x._2 + y._2, x._3 + y._3)).partitionBy(partitioner).persist()
    titles = title.keyBy(_._1).partitionBy(partitioner).persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    // If there is some value available for the given title ID, we calculate the average rating (since we have the sum and the total number of ratings)
    // If there is no entry, we return 0
    // Left-outer join provides a nice way of doing so
    titles.leftOuterJoin(aggRating).map(f => f._2._2 match {
      case Some(x) => (f._2._1._2, x._2 / x._3)
      case _ => (f._2._1._2, 0.0)
    })
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // Obtain titles containing all the keywords
    val filteredTitles: RDD[(Int, (Int, String, List[String]))] = titles.filter(f => {
      val intersectionWords: List[String] = keywords.filter(f._2._3.toSet)
      keywords.size == intersectionWords.size
    })
    // If no titles match the given keywords
    if(filteredTitles.isEmpty())
      return -1
    val ratedFilteredTitles = aggRating.join(filteredTitles)
    // If the matched titles are all unrated
    if(ratedFilteredTitles.isEmpty())
      return 0
    // Since we have an average of averages, each contributes equally, regardless of the number of ratings it individually received
    // Hence we initialise each with a weight of 1
    val onlyRatings: RDD[(Double, Int)] = ratedFilteredTitles.map(f => {
      (f._2._1._2 / f._2._1._3, 1)
    })
    val res: (Double, Int) = onlyRatings.reduce((x, y) => (x._1+ y._1, x._2 + y._2))
    res._1 / res._2
  }


  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val oldAggRating = aggRating
    aggRating = aggRating.map(f => {
      var curSum = f._2._2
      var curNum = f._2._3
      // foreach is the functional or "Scala" way of iterating over elements. Could have equivalently used a regular for loop
      delta.foreach(del => {
        if (f._1 == del._2) {
          // If there is no previous rating available
          if(del._3 == None){
            curSum = curSum + del._4
            curNum += 1
          }
          else{
            // If there was a rating available for that user and movie that we have to overwrite
            val prev:Double = del._3.get
            curSum = curSum- prev
            curSum =  curSum + del._4
          }
        }
      })
      (f._1, (f._1, curSum, curNum))

    })
    // Persisting the new one and un-persisting the old
    aggRating = aggRating.partitionBy(partitioner).persist()
    oldAggRating.unpersist()
  }
}
