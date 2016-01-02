package Data

import java.sql.DriverManager

class ConnectDb(db: String) {

  val connectionUrl = "jdbc:postgresql://localhost/%s?user=postgres&password=0134lbs".format(db)
  Class.forName("org.postgresql.Driver")
  val conn = DriverManager.getConnection(connectionUrl)

  def Open() = {
    conn
  }
  def Close(): Unit = {
    conn.close()
  }

}
