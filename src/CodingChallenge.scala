import scala.io.Source
import java.io._

object CodingChallenge extends App{

  val lines = Source.fromFile("smallSampleInput.csv").getLines().toStream
  val columnsLine #:: rows = lines
  
  val numColumns = columnsLine.split("""\|""").length
  val colNameToIndex: Map[String, Int] = columnsLine.split("""\|""").zipWithIndex.toMap
  type Table = Vector[Vector[Option[String]]]


  //Create a 2-d Vector of Option[Sting] values representing the table of values
  val rowsVec: Table = rows.map(row => {
    val rowSplit = row.split("""\|""").map( element => element match {
      case "" => None
      case _ => Some(element)
    }).toVector

    //Fill remaining (empty) values with None
    rowSplit ++ (rowSplit.length until numColumns).map(i => None).toVector
    }
  ).toVector

  def numUniqueCustomers = rowsVec
    .filter(row => row(colNameToIndex("CustID")) != None)
    .map(row => row(colNameToIndex("CustID")))
    .distinct
    .length

  def breakdownByElecOrGas = rowsVec
    .filter(row => row(colNameToIndex("ElecOrGas")) != None && row(colNameToIndex("CustID")) != None)
    .groupBy(row => row(colNameToIndex("CustID")))
    .mapValues(customerRows => customerRows map (customerRow => customerRow(colNameToIndex("ElecOrGas"))))
    .mapValues(elecOrGasVector => (elecOrGasVector contains Some("1"), elecOrGasVector contains Some("2")))
    .values
    .groupBy(v => v)
    .mapValues(v => v.toList.length)

  def breakdownByNumMeterReadings = {
    def breakdownByResource(resource: String) =
      rowsVec
        .filter(row => row(colNameToIndex("CustID")) != None && row(colNameToIndex("ElecOrGas")) == Some(resource))
        .map(row => row(colNameToIndex("CustID")))
        .groupBy(v => v)
        .mapValues(v => v.length).values
        .groupBy(v => v)
        .mapValues(v => v.toList.length)

    Map("1" -> breakdownByResource("1"), "2" -> breakdownByResource("2"))
  }

  def avgConsumptionPerMonth = {
    def breakdownByResource(resource: String) =
      rowsVec
        .filter(row => row(colNameToIndex("ElecOrGas")) == Some(resource) &&
        row(colNameToIndex("Bill Month")) != None && row(colNameToIndex("Consumption")) != None &&
        row(colNameToIndex("Bill Year")) != None)

        //One row is badly formed, with "N" as its month, filter these out
        .filter(row => row(colNameToIndex("Bill Month")) match {
          case Some(str) if str.matches("""\d{1,2}""") => true
          case _ => false
          })

        //One row has Bill Year value of "1", filter these out
        .filter(row => row(colNameToIndex("Bill Year")) match {
          case Some(str) => str.matches("""\d{4}""")
          case _ => false
        })

        .groupBy(row => row(colNameToIndex("Bill Month")))

        //map Some("1") -> List( sum_of_jan_2011, sum_of_jan_2012, ...)
        //    Some("2") -> List( sum_of_feb_2011, sum_of_feb_2009, ...)
        .mapValues(monthRows => {
          monthRows
            .groupBy(row => row(colNameToIndex("Bill Year")))
            .mapValues(monthAndYearRows =>
              monthAndYearRows.map(monthAndYearRow =>
                monthAndYearRow(colNameToIndex("Consumption")) match {
                  case Some(str) => str.toDouble
                  case _ => 0
                }
              ).sum
            ).values.toList
        })
        .mapValues(monthSums => monthSums.sum / monthSums.length )

    Map("1" -> breakdownByResource("1"), "2" -> breakdownByResource("2"))
  }

  def performAnalysis(): Unit = {
      val writer = new PrintWriter(new File("results.txt"))

      //Part 1
      writer.println("Number of unique customers:\n\t" + numUniqueCustomers)
      writer.println()

      //Part 2
      val elecOrGas = breakdownByElecOrGas
      writer.println("Breakdown by Electricity or Gas")
      if(elecOrGas contains (true, false)) writer.println("\tElectricity only:\n\t\t" + elecOrGas((true,false)))
      if(elecOrGas contains (false, true)) writer.println("\tGas only:\n\t\t" + elecOrGas((false,true)))
      if(elecOrGas contains (true, true)) writer.println("\tBoth only:\n\t\t" + elecOrGas((true,true)))
      writer.println()

      //part 3
      val numReadings = breakdownByNumMeterReadings
      val elecNumReadings = numReadings("1").toVector.sortBy(keyValPair => keyValPair._1)
      val gasNumReadings = numReadings("2").toVector.sortBy(keyValPair => keyValPair._1)

      writer.println("Breakdown by number of readings")

      writer.println("\tElectricity\n\t\tNumber of meter readings: Number of customers")
      elecNumReadings.foreach{ keyValPair =>
        writer.println("\t\t\t" + keyValPair._1 + ": " + keyValPair._2)
      }

      writer.println("\tGas\n\t\tNumber of meter readings: Number of customers")
      gasNumReadings.foreach{ keyValPair =>
        writer.println("\t\t\t" + keyValPair._1 + ": " + keyValPair._2)
      }

      writer.println()

      //part 4
      writer.println("Average consumption per Bill Month")

      val monthNumToName = Map(
        1 -> "January",   2 -> "February",  3 -> "March",     4 -> "April",
        5 -> "May",       6 -> "June",      7 -> "July",      8 -> "August",
        9 -> "September", 10 -> "October",  11 -> "November", 12 -> "December"
      )

      val avgConsumption = avgConsumptionPerMonth
      //This is dangerous if there are values of bill month that aren't convertible to ints,
      //however this doesn't seem to be the case for this data set
      val elecAvgConsumption = avgConsumption("1").toVector.sortBy(keyValPair => keyValPair._1.getOrElse("-1").toInt)
      val gasAvgConsumption = avgConsumption("2").toVector.sortBy(keyValPair => keyValPair._1.getOrElse("-1").toInt)

      writer.println("\tElectricity")
      elecAvgConsumption.foreach{
        keyValPair =>
          try {
            val monthNum = keyValPair._1.getOrElse("-1").toInt
            if (monthNumToName contains monthNum)
              writer.println("\t\t" + monthNumToName(monthNum) + " (all years): " + keyValPair._2)
          } catch {
            case e: Exception => None
          }
      }

      writer.println("\tGas")
      gasAvgConsumption.foreach{
        keyValPair =>
          try {
            val monthNum = keyValPair._1.getOrElse("-1").toInt
            if (monthNumToName contains monthNum)
              writer.println("\t\t" + monthNumToName(monthNum) + " (all years): " + keyValPair._2)
          } catch {
            case e: Exception => None
          }
      }

      writer.close()
  }

  performAnalysis()
}

