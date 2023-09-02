package app.recommender.collaborativeFiltering
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val ratings = ratingsRDD.map(f => Rating(f._1, f._2, f._4))
    // Using the library available
    model = ALS.train(ratings, rank, maxIterations, regularizationParameter, n_parallel, seed)
  }

  def predict(userId: Int, movieId: Int): Double =
    model.predict(userId, movieId)


}
