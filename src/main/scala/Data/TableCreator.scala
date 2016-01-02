package Data

import java.io.File
import java.sql.ResultSet

import scala.io.Source

object TableCreator {

  def CreateNewTable(db: String, file: File): Boolean = {
    val fileName = file.toString.split('\\').last.dropRight(4)
    val columns = GetColumns(file)
    val createStatement = GetCreateStatement(fileName, columns)
    val dropStatement = "DROP TABLE IF EXISTS %s"

    val conn = new ConnectDb(db).Open()
    val tran = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY ,ResultSet.CONCUR_READ_ONLY)

    tran.execute(dropStatement.format(fileName))

    if (createStatement.nonEmpty) {
      tran.execute(createStatement)
      println("----------------")
      println("table %s created successfully".format(fileName))
      conn.close()
      return true
    }

    println(s"problem creating table $fileName")
    conn.close()
    false
  }

  def GetColumns(file: File): Array[(String, String)] = {
    val fileData = Source.fromFile(file).getLines().take(2).toList
    val columnNames = fileData.head.split(',')
    val data = fileData.last.split(',')

    if (file.toString.split('\\').last.dropRight(4).startsWith("transactions") || file.toString.split('\\').last.dropRight(4).startsWith("universe")) {
      return columnNames map { a => (a, "VARCHAR(100)") }
    }

    columnNames zip data map { case (a, b) => if (a == "Injury_Type") (a, "VARCHAR(100)") else (a, GetDataType(b)) }
  }

  def GetDataType(n: String): String = isNumeric(n) match {
    case true => "INT"
    case false => "VARCHAR(100)"
  }

  def GetCreateStatement(fileName: String, columns: Array[(String, String)]): String = {
    val statement = "CREATE TABLE %s %s"

    def loop(c: Array[(String, String)], s: String): String = {
      if (c.isEmpty)
        s + ");"
      else if (c.length <= 1)
        s + clean(c.head._1) + " " + c.head._2 + ");"
      else {
        loop(c.tail, s + clean(c.head._1) + " " + c.head._2 + ",")
      }
    }

    statement.format(fileName, loop(columns, "("))
  }

  def isNumeric(n: String): Boolean = {
    if (n.length == 0)
      return false

    if (n.startsWith("-"))
      n.tail.forall(Character.isDigit)
    else
      n.forall(Character.isDigit)
  }

  def clean(columnName: String): String = {
    columnName.replace('-', '_')
      .replace('/', '_')
      .replace("4th", "fourth")
      .replace('(', '_')
      .replace(')', '_')
      .replace(".", "")
      .replace("3D", "third")
      .replace("+", "_plus_")
      .replace("40", "forty_")
      .replace("50", "fifty_")
      .replace("100", "hundred_")
      .replace("300", "three_hundred_")
      .replace("10", "ten_")
      .replace("20", "twenty_")
  }
}
