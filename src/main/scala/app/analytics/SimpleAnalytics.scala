package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = new HashPartitioner(20)
  private var moviesPartitioner: HashPartitioner = new HashPartitioner(20)

  // Global variables required for other functions
  private var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double, Int)]])] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    // Group all movies by their IDs, partition them onto different machines and persist them to avoid parsing for every operation involving their data
    titlesGroupedById = movie.groupBy(f => f._1).partitionBy(moviesPartitioner).persist()
    val temp: RDD[(Int, Iterable[(Int, Int, Option[Double], Double, Int)])] = ratings.groupBy(f => new DateTime(f._5.toLong * 1000).getYear)
    // Group all ratings first by their year and then by the movie IDs, partition them onto different machines and persist them to avoid parsing for every operation involving their data
    ratingsGroupedByYearByTitle = temp.map(f => (f._1, f._2.groupBy(g => g._2))).partitionBy(ratingsPartitioner).persist()
  }


  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    // Since we store ratings by year, we can just return the size of the storage for every year
    ratingsGroupedByYearByTitle.map(f => (f._1, f._2.values.size))
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val mostRatedIdEachYear: RDD[(Int, Int)] = ratingsGroupedByYearByTitle.map(f => {
      val yearRatings: Iterable[Iterable[(Int, Int, Option[Double], Double, Int)]] = f._2.values // of different movie ids
      var id: Int = 0
      var numRatings = 0
      // Iterating over the years.
      // This is not a very big loop because the total number of years available are anyway very few.
      for (movieRatings <- yearRatings) {
        val curRatings: Int = movieRatings.size
        // Rule 1 for getting the movie with the most number of ratings
        if (curRatings > numRatings) {
          numRatings = curRatings
          val tuple: (Int, Int, Option[Double], Double, Int) = movieRatings.head
          id = tuple._2
        }
        // Rule 2 for tie-breaking, required when there is a tie for most popular movie
        else if (curRatings == numRatings) {
          val tuple: (Int, Int, Option[Double], Double, Int) = movieRatings.head
          // Replace if current id is greater
          if (tuple._2 > id)
            id = tuple._2
        }
      }
      (id, f._1)
    })
    // We do a join using the title IDs to obtain the title names for returning
    val titlesRatingsJoin = mostRatedIdEachYear.join(titlesGroupedById)
    titlesRatingsJoin.map(f => {
      (f._2._1, f._2._2.head._2)
    })
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    val mostRatedIdEachYear: RDD[(Int, Int)] = ratingsGroupedByYearByTitle.map(f => {
      // Practically the same implementation as the above part
      val yearRatings: Iterable[Iterable[(Int, Int, Option[Double], Double, Int)]] = f._2.values
      var id: Int = 0
      var numRatings = 0
      for (movieRatings <- yearRatings) {
        val curRatings: Int = movieRatings.size
        if (curRatings > numRatings) {
          numRatings = curRatings
          val tuple: (Int, Int, Option[Double], Double, Int) = movieRatings.head
          id = tuple._2
        }
        else if (curRatings == numRatings) {
          val tuple: (Int, Int, Option[Double], Double, Int) = movieRatings.head
          if (tuple._2 > id)
            id = tuple._2
        }
      }
      (id, f._1)
    })
    val titlesRatingsJoin = mostRatedIdEachYear.join(titlesGroupedById)
    titlesRatingsJoin.map(f => {
      (f._2._1, f._2._2.head._3)
    })
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    // Since we want the global maximum and minimum, we have to collect the RDD
    val mostRatedGenre: Iterable[List[String]] = getMostRatedGenreEachYear.collectAsMap().values
    // Ratings for each genre
    val genreCounts:Map[String, Int] = mostRatedGenre.flatten.groupBy(identity).mapValues(x => x.size)
    // Ratings for each genre sorted
    val genreCountsSorted: Seq[(String, Int)] = genreCounts.toSeq.sortBy(f => (f._2, f._1))
    ((genreCountsSorted.head._1, genreCountsSorted.head._2), (genreCountsSorted.last._1, genreCountsSorted.last._2))
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    // Again, we need to collect because we want all the movies for a given set of genres
    val genresCollected: Array[String] = requiredGenres.collect()
    val moviesReq: RDD[(Int, String, List[String])] = movies.filter(f => {
      val genresList: List[String] = genresCollected.toList
      // Next two lines because all the genres must be present in a movie for it to be kept
      val filteredGenresList: List[String] = genresList.filter(f._3.toSet)
      filteredGenresList.size == genresList.size
    })
    moviesReq.map(_._2)
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    // Broad-casted the list of required genres to all nodes so we can filter on the same list on all devices
    val broadcastReqGenres: Broadcast[List[String]] = broadcastCallback(requiredGenres)
    val moviesReq: RDD[(Int, String, List[String])] = movies.filter(f => {
      val genresList:List[String] = broadcastReqGenres.value
      val filteredGenresList:List[String] = genresList.filter(f._3.toSet)
      filteredGenresList.size == genresList.size
    })
    moviesReq.map(_._2)
  }

}

