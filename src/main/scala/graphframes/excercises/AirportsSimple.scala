package graphframes.excercises

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object AirportsSimple {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("Airports").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    val verticles = List(1 -> "SFO", 2 -> "ORD", 3 -> "DFW")
    val edges = List((1, 2, 1800), (2, 3, 800), (3, 1, 1400))

    val verticesDF = spark.createDataFrame(verticles).toDF("id", "name")
    val edgesDF = spark.createDataFrame(edges).toDF("src", "dst", "distance")

    val graph = GraphFrame(verticesDF, edgesDF)

    println("Vertices:")
    graph.vertices.show()

    println("Edges:")
    graph.edges.show()

    // How many airports are there?
    val numAirports = graph.vertices.count
    println(s"Airports num = $numAirports")

    // How many routes are there?
    val numRoutes = graph.edges.count
    println(s"Routes num = $numRoutes")

    // How many routes are longer than 1000 miles?
    val longRoutes = graph.edges.filter("distance > 1000").count
    println(s"Routes longer than 1000 miles = $longRoutes")

    // Which routes are longer than 1000 miles?
    graph.edges.filter('distance > 1000).show

    // Triplets
    println("Triplets:")
    graph.triplets.show
  }
}
