package mysql.helloSql

import java.sql.{Connection, DriverManager}

/**
 * 需要在pom文件里面加入对应MySQL版本依赖哦
 */
class DBConnectionClass {
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/DavidYang"
  val username = "root"
  val password = "yang5662580dc"

  def readDataBase():Boolean = {
    try {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, username, password)
      println("connection: " + connection)
      true
    }
    catch {
      case _: Throwable => println("Could not connect to MySQL database.")
        false
    }
  }
}

object HelloMySQL {
  def main(args: Array[String]): Unit = {
    val dbConnectionObject = new DBConnectionClass
    val dbConnectionOFlag = dbConnectionObject.readDataBase()
    val message =  if (dbConnectionOFlag) {
      "MySQL connection succeed."
    } else {
      "MySQL connection failed."
    }
    println(message)
  }
}
