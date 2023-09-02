package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val fileContents = sc.textFile(getClass.getResource(path).getPath).persist(newLevel = StorageLevel.MEMORY_AND_DISK)

    // For every entry, store a user's ID, ID of the movie they rated, their previous rating if it exists, their current rating and the year
    fileContents.map{f => {
      val spl = f.split('|')
      val x: Option[Double] = None
      (spl(0).toInt, spl(1).toInt, x, spl(2).toDouble, spl(3).toInt)
    }}
  }
}