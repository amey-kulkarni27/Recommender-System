package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var avgUserRatingsMap: scala.collection.Map[Int, Double] = null
  private var globalAvgDeviationMap: scala.collection.Map[Int, Double] = null
  private var globalAvg: Double = 0.0
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val avgUserRatings = ratingsRDD.map(f => (f._1, f._4, 1)).keyBy(_._1).reduceByKey((x, y) => (x._1, x._2 + y._2, x._3 + y._3)).map(f => (f._1, f._2._2/f._2._3))
    globalAvg = avgUserRatings.map(f => (f._1, f._2, 1)).keyBy(_._1).reduceByKey((x, y) => (x._1, x._2 + y._2, x._3 + y._3)).map(f => f._2._2 / f._2._3).collect()(1)

    // Making the average rating of every user available to the ratingsRDD
    val ratingsWithAvg: RDD[(Int, Int, Double, Double)] = ratingsRDD.keyBy(_._1).join(avgUserRatings.keyBy(_._1)).map(f => (f._1, f._2._1._2, f._2._1._4, f._2._2._2))
    // Calculating normalised and then global average deviation according to the formulas given
    val normalisedDeviation: RDD[(Int, Double, Int)] = ratingsWithAvg.map(f => {
      var scale:Double = 0
      if(f._3 == f._4)
        scale = 1
      else if(f._3 > f._4)
        scale = 5 - f._4
      else
        scale = f._4 - 1
      (f._2, (f._3 - f._4) / scale, 1)
    })
    // A map giving the average rating for every user
    avgUserRatingsMap = avgUserRatings.collectAsMap()
    val globalAvgDeviation = normalisedDeviation.keyBy(_._1).reduceByKey((x, y) => (x._1, x._2 + y._2, x._3 + y._3)).map(f => (f._1, f._2._2/f._2._3))
    // A map giving the global average deviation in rating for every movie
    globalAvgDeviationMap = globalAvgDeviation.collectAsMap()
    // Importantly, we prepare and collect the maps in init itself so the prediction does not take time in collecting the data
    // This is because the prediction happens iteratively, for every userId and movieId one by one.
  }

  def predict(userId: Int, movieId: Int): Double = {
    // If the user has not rated any movies, we return the global average
    val reqUserRatingValue: Double = avgUserRatingsMap.getOrElse(userId, globalAvg)
    if(!avgUserRatingsMap.keySet.exists(_ == userId))
      return reqUserRatingValue
    // If the movie has no rating available, we simply return the average rating given by that user
    if(!globalAvgDeviationMap.keySet.exists(_ == movieId))
      return reqUserRatingValue
    // We use the same formula that was presented before
    val reqAvgDeviationValue: Double = globalAvgDeviationMap.getOrElse(movieId, reqUserRatingValue)
    val x: Double = reqUserRatingValue + reqAvgDeviationValue
    var sc: Double = 0
    if(x > reqUserRatingValue)
      sc = 5 - reqUserRatingValue
    else if(x < reqUserRatingValue)
      sc = reqUserRatingValue - 1
    else
      sc = 1
    reqUserRatingValue + reqAvgDeviationValue*sc
  }
}
