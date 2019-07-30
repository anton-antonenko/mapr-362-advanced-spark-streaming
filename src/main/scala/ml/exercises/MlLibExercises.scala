package ml.exercises

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object MlLibExercises {

  case class Rating(userId: Int, movieId: Int, rating: Float)
  object Rating {
    def apply(str: String): Rating = {
      val vals = str.split("::")
      assert(vals.size == 4)

      new Rating(vals(0).toInt, vals(1).toInt, vals(2).toFloat)
    }
  }

  case class Movie(movieId: Int, title: String)
  object Movie {
    def apply(str: String): Movie = {
      val vals = str.split("::")
      assert(vals.size == 3)

      new Movie(vals(0).toInt, vals(1))
    }
  }

  case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)
  object User {
    def apply(str: String): User = {
      val vals = str.split("::")
      assert(vals.size == 5)

      new User(vals(0).toInt, vals(1), vals(2).toInt, vals(3).toInt, vals(4))
    }
  }

  case class Flight(dofM: String, dofW: String, carrier: String, tailnum: String, flnum: Int, org_id: String, origin: String,
                    dest_id: String, dest: String, crsdeptime: Double, deptime: Double,
                    depdelaymins: Double, crsarrtime: Double, arrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Int)
  object Flight {
    def apply(str: String): Flight = {
        val line = str.split(",")
        Flight(line(0), line(1), line(2), line(3), line(4).toInt, line(5),
          line(6), line(7), line(8), line(9).toDouble, line(10).toDouble,
          line(11).toDouble, line(12).toDouble, line(13).toDouble,
          line(14).toDouble, line(15).toDouble, line(16).toInt)
    }
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("MlLibExercises").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    if (false) {
      // MOVIE RATINGS

      val ratingsDF = spark.read.textFile("./data/ratings.dat").map(Rating.apply).toDF()
      val usersDF = spark.read.textFile("./data/users.dat").map(User.apply).toDF()
      val moviesDF = spark.read.textFile("./data/movies.dat").map(Movie.apply).toDF()

      ratingsDF.createTempView("ratings")
      usersDF.createTempView("users")
      moviesDF.createTempView("movies")

      // Get the maximum and minimum ratings along with the count of users who have rated a movie.
      // Display the top 20 rows including the title, max rating, min rating, and number of users:
      spark.sql(
        """
        select m.title, stats.movieId, stats.maxR, stats.minR, stats.ratesCnt
        from (select r.movieId as movieId, max(r.rating) maxR, min(r.rating) minR, count(distinct r.userId) as ratesCnt
              from ratings r
              group by r.movieId) as stats, movies m
        where m.movieId = stats.movieId
        order by stats.ratesCnt desc
      """).show(truncate = false)


      // Show the ten users who rated the most movies
      spark.sql(
        """
        select userId, count(*) as ratesCnt
        from ratings
        group by userId
        order by ratesCnt desc
        limit 10
      """).show(truncate = false)


      // User 4169 rated the most movies. Which movies did user 4169 rate higher than four?
      spark.sql(
        """
        select m.title, r.rating
        from ratings r, movies m
        where r.movieId = m.movieId and r.userId = 4169 and r.rating >= 4
      """).show(truncate = false)


      // Predictions
      val splits = ratingsDF.randomSplit(Array(0.8, 0.2), 0L)
      val trainingDS = splits(0).cache()
      val testDS = splits(1).cache()

      val model = new ALS()
        .setMaxIter(10)
        .setRank(20)
        .setRegParam(0.01)
        .setUserCol("userId")
        .setItemCol("movieId")
        .setRatingCol("rating")
        .fit(trainingDS)

      val predictionsDf = model.transform(testDS)

      testDS.createTempView("test")
      predictionsDf.createTempView("predictions")

      val comparison = spark.sql(
        """
        select t.userId as userId, t.movieId as movieId, t.rating as rating, p.prediction as prediction, abs(t.rating - p.prediction) as difference
        from test t, predictions p
        where t.userId = p.userId and t.movieId = p.movieId
      """)

      comparison.createTempView("comparison")
      comparison.show(truncate = false)

      // count false positives
      spark.sql(
        """
        select count(*) as false_positives_count
        from comparison
        where difference >= 3 and prediction > 0
      """).show(truncate = false)


      // Get the top four movie prediction for user 4169
      spark.sql(
        """
             select userId, movieId, prediction
             from predictions
             where userId = 4169
             order by prediction desc
             limit 4
      """)
    } else {
      // FLIGHTS

      val flightsDS = spark.read.textFile("./data/flights/rita2014jan.csv").map(Flight.apply).cache()

      // Transform the non-numeric features into numeric values. For example, the carrier AA is the
      // number 6, and the originating airport ATL is 273.
      // Here, transform the carrier:
      val carrierMap = flightsDS.map(flight => flight.carrier).distinct.collect.zipWithIndex.toMap

      // Transform the originating and destination airports:
      val originMap = flightsDS.map(flight => flight.origin).distinct.collect.zipWithIndex.toMap
      val destMap = flightsDS.map(flight => flight.dest).distinct.collect.zipWithIndex.toMap

      // Next, create a DS containing feature arrays consisting of the label and the features in numeric format.
      // delayed, dofM, dofW, crsDepTime, crsArrTime, carrier, elapTime, origin, dest
      val mlprep = flightsDS.map(flight => {
        val monthday = flight.dofM.toInt - 1 // category
        val weekday = flight.dofW.toInt - 1 // category
        val crsdeptime1 = flight.crsdeptime.toInt
        val crsarrtime1 = flight.crsarrtime.toInt
        val carrier1 = carrierMap(flight.carrier) // category
        val crselapsedtime1 = flight.crselapsedtime
        val origin1 = originMap(flight.origin) // category
        val dest1 = destMap(flight.dest) // category
        val delayed = if (flight.depdelaymins > 40) 1.0 else 0.0
        Array(delayed.toDouble, monthday.toDouble, weekday.toDouble,
          crsdeptime1.toDouble, crsarrtime1.toDouble, carrier1.toDouble,
          crselapsedtime1.toDouble, origin1.toDouble, dest1.toDouble)
      })
      mlprep.show(truncate = false)

      // Create a Dataset containing arrays of LabeledPoints.
      // A labeled point is a class that represents the feature vector and a label of a data point.
      val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))))
      mldata.show(truncate = false)

      // Split the data to get a good percentage of delayed and not delayed flights. Then split it into a
      // training data set and a test data set:
      // mldata0 is %85 not delayed flights
      val mldata0 = mldata.filter(_.label == 0).randomSplit(Array(0.85, 0.15))(1)
      // mldata1 is %100 delayed flights
      val mldata1 = mldata.filter(_.label != 0)
      // mldata2 is delayed and not delayed
      val mldata2 = mldata0.union(mldata1)

      // Split mldata2 into training and test data
      val mldataSplits = mldata2.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (mldataSplits(0), mldataSplits(1))

      // Create a model
      val dt = new DecisionTreeClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxDepth(9)
        .setImpurity("gini")
        .setMaxBins(7000)

      val dtModel = dt.fit(trainingData)

      // Evaluate the model on test instances, and compute the test error:
      val labelAndPreds = dtModel.transform(testData)
      labelAndPreds.createTempView("labelAndPreds")
      labelAndPreds.show()

      val wrongPredictions = spark.sql("select count(*) from labelAndPreds where label != prediction")

      val wrongPredictionCount = wrongPredictions.count()
      val predictionsCount = labelAndPreds.count()
      val ratioWrong = wrongPredictionCount.toDouble / predictionsCount
      println(s"Predictions: $predictionsCount. Wrong predictions: $wrongPredictionCount. Wrong predictions ratio: $ratioWrong")

    }
  }
}
