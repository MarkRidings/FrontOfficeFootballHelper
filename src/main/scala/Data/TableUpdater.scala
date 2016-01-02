package Data

import java.io.File
import java.sql.ResultSet

import scala.io.Source

object TableUpdater {

  def UpdateTable(db: String, file: File): Unit = {
    val insertStatement = GetInsertStatement(file)
    val conn = new ConnectDb(db).Open()
    val tableName = "%s".format(file.toString.split('\\').last.dropRight(4))

    val tran = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    if (insertStatement._2 > 0) {
     try {
       println("Getting ready to insert %d into %s".format(insertStatement._2, tableName))
       tran.executeUpdate(insertStatement._1)
     }
      catch {
        case e: Exception => {
          println("problem with inserting into %s".format(tableName))
          println(e.getMessage)
        }
      }
    }

    println("%d entries inserted into table %s".format(insertStatement._2, tableName))
    conn.close()
  }

  def GetInsertStatement(file: File): (String, Int) = {
    val tableName = file.toString.split('\\').last.dropRight(4)
    val fileData = Source.fromFile(file.toString).getLines()
    val insertStarter = new StringBuilder
    insertStarter.append("INSERT INTO %s VALUES".format(tableName))

    var count = 0
    fileData.drop(1).foreach(x => {
      insertStarter.append("(%s),".format(clean(x)))
      count += 1
    })

    (insertStarter.toString().dropRight(1) + ";", count)
  }

  def clean(s: String): String = {
    s.split(',').map(i => "'%s'".format(i.replaceAll("'", ""))).mkString(",")
  }
}
