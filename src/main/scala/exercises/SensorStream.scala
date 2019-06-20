package exercises

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object SensorStream {

  def main(args: Array[String]): Unit = {
    // set up able configuration
    val spark = SparkSession.builder.master("local").appName("SensorStream").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // schema for sensor data
    val sensorSchema = new StructType()
      .add("resid", "string")
      .add("date", "string")
      .add("time", "string")
      .add("hz", "double")
      .add("disp", "double")
      .add("flow", "double")
      .add("sedPPM", "double")
      .add("psi", "double")
      .add("chlppm", "double")

    // TO DO: parse the lines of data into sensor objects
    val sensorStream = spark.readStream
      .option("sep", ",")
      .option("header", "false")
      .schema(sensorSchema)
      .csv("./data/stream")

    sensorStream.createTempView("sensor")

    // max min vals
    val sensorStats = spark.sql("SELECT resid, date, " +
      "MAX(hz) as maxhz, min(hz) as minhz, avg(hz) as avghz, " +
      "MAX(disp) as maxdisp, min(disp) as mindisp, avg(disp) as avgdisp, " +
      "MAX(flow) as maxflow, min(flow) as minflow, avg(flow) as avgflow, " +
      "MAX(sedPPM) as maxsedPPM, min(sedPPM) as minsedPPM, avg(sedPPM) as avgsedPPM, " +
      "MAX(psi) as maxpsi, min(psi) as minpsi, avg(psi) as avgpsi, " +
      "MAX(chlPPM) as maxchlPPM, min(chlPPM) as minchlPPM, avg(chlPPM) as avgchlPPM " +
      "FROM sensor GROUP BY resid, date")

    //println("sensor max, min, averages:")
    val sensorStatsQuery = sensorStats
      .writeStream
      .outputMode("complete")
      .option("truncate", "false")
      .format("console")
      .start()


    // TO DO: Filter sensor data for low psi (< 5.0)
    val lowPsi = sensorStream.filter("psi < 5")

    // TO DO: Start the computation and save alerts as CSV file
    //println("Low PSI:")
    val lowPsiQuery = lowPsi
      .writeStream
      .outputMode("append")
      .option("truncate", "false")
      .format("console")
      .start()


    // read pump maintenance and vendor data
    val maintenanceData = spark
      .read
      .option("sep", ",")
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("./data/sensormaint.csv")
      .toDF("resid", "eventDate", "technician", "description")

    val vendorData = spark
      .read
      .option("sep", ",")
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("./data/sensorvendor.csv")
      .toDF("resid", "pumpType", "purchaseDate", "serviceDate", "vendor", "longitude", "lattitude")

    maintenanceData.createTempView("maintenance")
    vendorData.createTempView("vendor")
    lowPsi.createTempView("lowPsi")


    //println("Maintenance info on pumps with low PSI:")
    val maintenance = spark.sql("""
              select s.resid, s.date, s.psi, v.pumpType, v.vendor, m.eventDate as maintenanceEventDate, m.technician
              from lowPsi s
              join vendor v on s.resid = v.resid
              join maintenance m on s.resid = m.resid
              """)

    val maintenanceQuery = maintenance
      .writeStream
      .option("truncate", "false")
      .format("console")
      .start()



    //TO DO: Wait for the computation to terminate
    val _300_secs = TimeUnit.MILLISECONDS.convert(300, TimeUnit.SECONDS)

    sensorStatsQuery.awaitTermination(_300_secs)
    lowPsiQuery.awaitTermination(_300_secs)
    maintenanceQuery.awaitTermination(_300_secs)
  }

}