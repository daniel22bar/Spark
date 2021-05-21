package spark.task

import org.apache.log4j.{Level, Logger}

object Start {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "D:\\Program File D\\winutils")

    TripsAndDrivers.start()
 }
}
