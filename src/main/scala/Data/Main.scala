package Data

import java.io.{FilenameFilter, File}

object Main {

  def main(args: Array[String]): Unit = {



    val initPath = "C:\\Users\\MarkR\\AppData\\Roaming\\Solecismic Software\\Front Office Football Seven\\leaguedata"
    val file = new File(initPath)

    val directories = file.list(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        new File(dir, name).isDirectory
      }
    })

    var i = 1
    println("Select league")
    directories.foreach(x => {
      println("%d - %s".format(i, x))
      i += 1
    })

    val choice = Console.readInt() - 1
    println("you selected: %s".format(directories.apply(choice)))



    val filePath = initPath + "\\" + directories.apply(choice)
    val fileList = new File(filePath).listFiles().filter(_.getName.endsWith(".csv"))

    fileList.foreach { f =>
      TableCreator.CreateNewTable(directories.apply(choice), f)
      TableUpdater.UpdateTable(directories.apply(choice), f)
    }


  }

}
