package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val fileContents = sc.textFile(getClass.getResource(path).getPath).persist(newLevel = StorageLevel.MEMORY_AND_DISK)

    // For every entry, store a movie's ID, Name and its list of Genres
    fileContents.map(f =>{
    val spl = f.split('|')
    val len = spl.length
    val id = spl(0).toInt
    val name = spl(1).substring(1, spl(1).length - 1)
    var genres: Array[String] = new Array[String](len-2)

    // Edge case when there is only 1 Genre for a given movie
    if(len == 3){
      genres(0) = spl(2).substring(1, spl(2).length - 1)
    }
    else{
      // Iterate over the movie genres one by one
      for (i <- 2 to len - 1) {
        if (i == 2) {
          genres(i - 2) = spl(i).substring(1)
        }
        else if (i == len - 1) {
          genres(i - 2) = spl(i).substring(0, spl(i).length - 1)
        }
        else {
          genres(i - 2) = spl(i)
        }
      }
    }

    (id, name, genres.toList)})
  }
}
