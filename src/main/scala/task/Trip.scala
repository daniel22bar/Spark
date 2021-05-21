package spark.task

import java.time.LocalDate

case class Trip(driverID:Int = 0, city:String = "", distance:Int = -1, date: LocalDate = LocalDate.now()){

}



