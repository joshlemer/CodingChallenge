import scala.io.Source
import java.io._
import scala.collection.mutable
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

  def breakdownByNumMeterReadings = rowsVec
    .filter(row => row(colNameToIndex("CustID")) != None)
    .map(row => row(colNameToIndex("CustID")))
    .groupBy(v => v)
    .mapValues(v => v.length).values
    .groupBy(v => v)
    .mapValues(v => v.toList.length)


  def avgConsumptionPerMonth = {
    def analyzeByResource(resource: String) =
      rowsVec
        .filter(row => row(colNameToIndex("ElecOrGas")) == Some(resource) && row(colNameToIndex("Bill Month")) != None && row(colNameToIndex("Consumption")) != None)

        //One row is badly formed, with "N" as its month, filter these out
        .filter(row => row(colNameToIndex("Bill Month")) match {
          case Some(str) if str.matches("""\d{1,2}""") => true
          case _ => false
          })

        .groupBy(row => row(colNameToIndex("Bill Month")))
        .mapValues(monthRows =>
          monthRows.map(monthRow =>
            monthRow(colNameToIndex("Consumption")) match {
              case Some(str) => str.toDouble
              case None => throw new Exception("This row contains no consumption value")
            }).sum)

    Map("1" -> analyzeByResource("1"), "2" -> analyzeByResource("2"))
  }

  def performAnalysis(): Unit = {
      val writer = new PrintWriter(new File("results.txt"))
      //writer.write("foo")
      writer.close()
  }

  performAnalysis()

}

