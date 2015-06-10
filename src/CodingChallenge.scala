import scala.io.Source
import java.io._
import scala.collection.mutable
object CodingChallenge extends App{

  val lines = Source.fromFile("smallSampleInput.csv").getLines().toStream
  val columnsLine = lines.head
  val numColumns = columnsLine.split("""\|""").length
  val rows = lines.tail
  val columnIndexes: Map[Int, String] = (0 until numColumns).map(i => i -> columnsLine.split("""\|""").toList(i)).toMap
  val columnNameToIndex: Map[String, Int] = columnsLine.split("""\|""").zipWithIndex.toMap
  type Table = Vector[Vector[Option[String]]]

  val writer = new PrintWriter(new File("test.txt"))
  writer.write(rows.head.split("""\|""").toList.length.toString)
  writer.close()

  //Return a 2-d Vector of Option[Sting] values
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
    .filter(row => row(columnNameToIndex("CustID")) != None)
    .map(row => row(columnNameToIndex("CustID")))
    .distinct
    .length

  def breakdownByElecOrGas = rowsVec
    .filter(row => row(columnNameToIndex("ElecOrGas")) != None && row(columnNameToIndex("CustID")) != None)
    .groupBy(row => row(columnNameToIndex("CustID")))
    .mapValues(customerRows => customerRows map (customerRow => customerRow(columnNameToIndex("ElecOrGas"))))
    .mapValues(elecOrGasVector => (elecOrGasVector contains Some("1"), elecOrGasVector contains Some("2")))
    .values
    .groupBy(v => v)
    .mapValues(v => v.toList.length)

  def breakdownByNumMeterReadings = rowsVec
    .filter(row => row(columnNameToIndex("CustID")) != None)
    .map(row => row(columnNameToIndex("CustID")))
    .groupBy(v => v)
    .mapValues(v => v.length).values
    .groupBy(v => v)
    .mapValues(v => v.toList.length)


  def avgConsumptionPerMonth = {
    //Do Elec first
    rowsVec
      .filter(row => row(columnNameToIndex("ElecOrGas")) == Some("1") && row(columnNameToIndex("Bill Month")) != None && row(columnNameToIndex("Consumption")) != None)

      //One row is badly formed, with "N" as its month, filter these out
      .filter(row => row(columnNameToIndex("Bill Month")) match {
        case Some(str) if str.matches("""\d{1,2}""") => true
        case _ => false
        })

      .groupBy(row => row(columnNameToIndex("Bill Month")))
      .mapValues(monthRows =>
        monthRows.map(monthRow =>
          monthRow(columnNameToIndex("Consumption")) match {
            case Some(str) => str.toDouble
            case None => throw new Exception("This row contains no consumption value")
          }).sum)
  }
  println(avgConsumptionPerMonth)

}
