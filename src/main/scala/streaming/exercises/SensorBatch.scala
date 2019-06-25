package streaming.exercises

import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.sql.types.StructType

object SensorBatch {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("SensorBatch").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // schema for sensor data
    val userSchema = new StructType()
      .add("resid", "string")
      .add("date", "string")
      .add("time", "string")
      .add("hz", "double")
      .add("disp", "double")
      .add("flow", "double")
      .add("sedPPM", "double")
      .add("psi", "double")
      .add("chlppm", "double")


    val sensorDF = spark.read
      .option("sep", ",")
      .option("header", "false")
      .schema(userSchema)
      .csv("./data/sensordata.csv")

    sensorDF.show(20)
    println(sensorDF.count())

    // TO DO: Filter sensor data for low psi (< 5.0)
    val lowPsi = sensorDF.filter("psi < 5")

    lowPsi.printSchema()
    lowPsi.show(20)

    // group by ID and show avg PSI
    sensorDF
      .groupBy("resid")
      .agg(f.avg("psi"))
      .show()

    // create temp view for sensorData and select max,min,avg for all sensor ids
    sensorDF.createTempView("sensor")

    spark.sql("select resid, date, " +
      "max(hz), min(hz), round(avg(hz), 3) as avg_hz, " +
      "max(disp), min(disp), round(avg(disp), 3) as avg_disp, " +
      "max(flow), min(flow), round(avg(flow), 3) as avg_flow, " +
      "max(sedPPM), min(sedPPM), round(avg(sedPPM), 3) as avg_sedPPM, " +
      "max(psi), min(psi), round(avg(psi), 3) as avg_psi, " +
      "max(chlppm), min(chlppm), round(avg(chlppm), 3) as avg_chlppm " +
      "from sensor group by resid, date").show()


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


    println("Maintenance info on pumps with low PSI:")
    spark.sql("""
              select s.resid, s.date, s.psi, v.pumpType, v.vendor, m.eventDate as maintenanceEventDate, m.technician
              from lowPsi s
              join vendor v on s.resid = v.resid
              join maintenance m on s.resid = m.resid
              """)
      .show()
  }

}
