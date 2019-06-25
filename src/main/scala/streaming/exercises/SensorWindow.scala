package streaming.exercises

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, functions => f}

object SensorWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("SensorStreamWindow").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Schema for sensor data
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

    // TO DO: parse the lines of data into sensor objects
    val sensorStream = spark.readStream
      .option("sep", ",")
      .option("header", "false")
      .schema(userSchema)
      .csv("./data/stream")

    sensorStream.createTempView("sensor")

    import spark.implicits._

    // Start computation
    println("start streaming")

    // What is the count of sensor events by pump ID?
    val res = sensorStream
      .withColumn("datetime", f.to_timestamp(f.concat('date, f.lit(" "), 'time), "MM/dd/yy hh:mm"))
      //.withWatermark("datetime", "30 seconds")  // needed for csv
      //.groupBy('resid, 'date, f.window('time, "10 minutes", "5 minutes"))
      .groupBy('resid, f.window('datetime, "12 hours", "6 hours"))
      .agg(f.count('resid).alias("total_events"))

    val streamConsole = res
      .orderBy('window)
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    // What is the maximum, minimum, and average for PSI?
    val res2 = sensorStream
      .withColumn("datetime", f.to_timestamp(f.concat('date, f.lit(" "), 'time), "MM/dd/yy hh:mm"))
      .groupBy('resid, 'date, f.window('datetime, "10 minutes", "5 minutes"))
      .agg(f.max('psi), f.min('psi), f.avg('psi))

    // SQL version
    val res2SQL = spark
      .sql(
        """
          SELECT resid, date,
                 window(to_timestamp(concat(date, ' ', time), 'MM/dd/yy hh:mm'), '10 minutes', '5 minutes') as window,
                 max(psi) as maxpsi, min(psi) as minpsi, avg(psi) as avgpsi
          FROM sensor
          GROUP BY resid, date, window
          """)


    val stream2Console = res2SQL
      .orderBy('window)
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    /*val streamFile = res
      .select('datetime, 'resid, 'total_events)
      .writeStream
      .outputMode("append")
      .format("csv")
      .option("checkpointLocation", "./checkpoint2")
      .start("./result2")*/

    val _300_secs = TimeUnit.MILLISECONDS.convert(300, TimeUnit.SECONDS)

    streamConsole.awaitTermination(_300_secs)
    stream2Console.awaitTermination(_300_secs)
    //streamFile.awaitTermination()
  }

}
