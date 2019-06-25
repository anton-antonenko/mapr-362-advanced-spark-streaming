package graphframes.excercises

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

object AirportsReal {

  case class Flights(dofM: String, dofW: String, carrier: String,
                     tailnum: String, flnum: Int, org_id: Long, origin: String,
                     dest_id: Long, dest: String, crsdeptime: Double, deptime: Double,
                     depdelaymins: Double, crsarrtime: Double, arrtime: Double,
                     arrdelay: Double, crselapsedtime: Double, dist: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Airports").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val flightsDF = spark.read
      .option("inferSchema", "true")
      .csv("./data/flights/rita2014jan.csv")
      .toDF("dofM", "dofW", "carrier",
        "tailnum", "flnum", "org_id", "origin",
        "dest_id", "dest", "crsdeptime", "deptime",
        "depdelaymins", "crsarrtime", "arrtime",
        "arrdelay", "crselapsedtime", "dist")
      .as[Flights]
      .cache()

    println("Flights data:")
    flightsDF.show()


    // Airports are defined as vertices; each vertex has an ID and a property that is the airport name (a
    // String). Define an airports DataFrame with the ID and airport name, and show the first element:
    val airportsDF = flightsDF.select('org_id, 'origin).distinct.toDF("id", "name")
    println("Airports:")
    airportsDF.show()

    // Define a routes DataFrame with the properties (src id, dest id, distance), and show
    // the first two elements.
    val routesDF = flightsDF
      .select('org_id, 'dest_id, 'dist)
      .distinct
      .toDF("src", "dst", "distance")
    println("Routes:")
    routesDF.show()

    // Create a property graph called graph and show the first two vertices and edges:
    val graph = GraphFrame(airportsDF, routesDF)
    graph.vertices.show(2)
    graph.edges.show(2)

    // How many airports are there?
    val airportsNum = graph.vertices.count()
    println(s"Airports num = $airportsNum")

    // How many routes are there?
    val routesNum = graph.edges.count()
    println(s"Routes num = $routesNum")

    // Which routes are longer than 1000 miles?
    val longRoutes = graph.edges.filter('distance > 1000).sort(desc("distance"))
    val longRoutesNum = longRoutes.count()
    println(s"Routes longer than 1000 miles: $longRoutesNum")
    longRoutes.show()

    // Display triplets for all edges of graph:
    println("Triplets:")
    graph.triplets.show()

    // Compute the highest degree vertex:
    println("The highest in/out/all degree vertex:")
    graph.inDegrees.sort(desc("inDegree")).show(1)
    graph.outDegrees.sort(desc("outDegree")).show(1)
    graph.degrees.sort(desc("degree")).show(1)

    // Which three airports have the most incoming flights?
    println("Which three airports have the most incoming flights?")
    graph.inDegrees
      .join(airportsDF, graph.inDegrees("id") === airportsDF("id"))
      .sort(desc("inDegree"))
      .select('name, airportsDF("id"), 'inDegree)
      .show(3)

    // Which three airports have the most outgoing flights?
    println("Which three airports have the most outgoing flights?")
    graph.outDegrees
      .join(airportsDF, graph.outDegrees("id") === airportsDF("id"))
      .sort(desc("outDegree"))
      .select('name, airportsDF("id"), 'outDegree)
      .show(3)

    // What are the top 10 flights by distance from airport to airport?
    println("What are the top 10 flights by distance from airport to airport?")
    airportsDF.createTempView("airports")
    routesDF.createTempView("routes")

    spark.sql("""
        select s.name as source, d.name as destination, r.distance as distance
        from routes r, airports s, airports d
        where r.src = s.id and r.dst = d.id order by r.distance desc
      """)
      .dropDuplicates("distance")
      .sort(desc("distance"))
      .show(10)

    // OR

    spark.sql("""
        select concat('distance from ', s.name, ' to ', d.name, ' is: ', r.distance) as route
        from routes r, airports s, airports d
        where r.src = s.id and r.dst = d.id order by r.distance desc
      """).show(10, false)


  }
}
