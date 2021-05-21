package spark.task
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Arrays

object TripsAndDrivers {

  def start(): Unit ={
    val sparkConf = new SparkConf().setAppName("Hello spark from scala").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val driverRdd: RDD[String] = sc.textFile("data/taxi/drivers.txt")
    val tripsRdd: RDD[String] = sc.textFile("data/taxi/trips.txt")

    /*******************  Question 1 ***************************/
    println(s"total lines in file: ${tripsRdd.count()}")

    /*******************  Question 2 + 3 ***************************/
    val allTripsToBoston = getRddOfTripsTo(tripsRdd,"BOSTON")

    val longTripToBoston:Long = allTripsToBoston.unpersist().filter(_.distance>10).count()

    val milesToBoston:Double = allTripsToBoston.unpersist().map(_.distance).sum()

    println(s"there are: ${longTripToBoston} trips to Boston that take mote then 10 miles")
    println(s"the total distance of trips to boston is ${milesToBoston}")

    /*******************  Question 4 ***************************/
    val drivers: RDD[(Int, String)] = driverRdd.map(x => getDriver(x))
      .map(x => (x.driverID, x.name))

    val trips: RDD[(Int, Int)] = tripsRdd.map(x => getTrip(x))
      .groupBy(x=>x.driverID).mapValues(x=> x.map(_.distance).sum)

    println("the names of the 3 drivers that drive the most distance are: ")
    drivers.join(trips).sortBy(x=> x._2._2,false).take(3)
      .foreach(x=>println(x._2._1))

  }



  private def getRddOfTripsTo(rdd: RDD[String],city:String): RDD[Trip] ={
    rdd.map(t => getTrip(t))
      .filter(trip => trip.city.toUpperCase() == "BOSTON")
      .persist(StorageLevel.MEMORY_ONLY)
  }

  private def getDriver(string: String): Driver ={
    val strings: Array[String] = Arrays.copyOfRange(string.split(","), 0, 4)
    Driver(Integer.parseInt(strings(0).trim),strings(1).trim,strings(2).trim,strings(3))
  }

  private def getTrip(string: String): Trip ={
    val trip: Array[String] = Arrays.copyOfRange(string.split(" "), 0, 4)
    Trip(Integer.parseInt(trip(0)),trip(1),Integer.parseInt(trip(2)))
  }

}
