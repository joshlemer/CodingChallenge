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

  println(numUniqueCustomers)

}
