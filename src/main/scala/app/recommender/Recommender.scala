package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // Creating an RDD of the list of keywords/genres
    val genreRDD: RDD[List[String]] = sc.parallelize(List(genre))
    // Getting an RDD of a list of similar movies using the lookup function defined
    val similarMovies: RDD[List[(Int, String, List[String])]] = nn_lookup.lookup(genreRDD).map(f => f._2)
    // Collecting all similar movies
    val similarMoviesCollected:Array[List[(Int, String, List[String])]] = similarMovies.collect()
    // Creating an RDD of only the title IDs of the movies
    val similarMoviesRDD: RDD[Int] = sc.parallelize(similarMoviesCollected(0)).map(f => f._1)
    // Getting all the movies seen by the user. Using distinct to avoid duplicates (implementation detail)
    val userMovieSeen: RDD[Int] = ratings.filter(_._1 == userId).map(f => f._2).distinct()
    // Since we only want to recommend unseen movies, we subtract all the seen movies from the set of all similar movies
    val similarUnseenMoviesRDD: RDD[Int] = similarMoviesRDD.subtract(userMovieSeen)
    val similarUnseenMovies: Array[Int] = similarUnseenMoviesRDD.collect()
    // Now we get every movie ID with a predicted score
    val predictions:Array[(Int, Double)] = similarUnseenMovies.map(f => (f, baselinePredictor.predict(userId, f)))
    // We return the top K movies with the highest predicted ratings the user
    predictions.sortBy(f => -f._2).take(K).toList
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // The implementation is almost identical to the above
    val genreRDD: RDD[List[String]] = sc.parallelize(List(genre))
    val similarMovies: RDD[List[(Int, String, List[String])]] = nn_lookup.lookup(genreRDD).map(f => f._2)
    val similarMoviesCollected: Array[List[(Int, String, List[String])]] = similarMovies.collect()
    val similarMoviesRDD: RDD[Int] = sc.parallelize(similarMoviesCollected(0)).map(f => f._1)
    val userMovieSeen: RDD[Int] = ratings.filter(_._1 == userId).map(f => f._2).distinct()
    val similarUnseenMoviesRDD: RDD[Int] = similarMoviesRDD.subtract(userMovieSeen)
    val similarUnseenMovies: Array[Int] = similarUnseenMoviesRDD.collect()
    val predictions: Array[(Int, Double)] = similarUnseenMovies.map(f => (f, collaborativePredictor.predict(userId, f)))
    predictions.sortBy(f => -f._2).take(K).toList
  }
}
