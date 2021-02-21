package io.keepcoding.spark.exercise.provisioner

import java.sql.{Connection, DriverManager}

object JdbcProvisioner {

  def main(args: Array[String]) {
    val IpServer = "35.225.198.88"

    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$IpServer:5433/postgres"
    val username = "postgres"
    val password = "keepcoding"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")

      println("Creando la tabla metadata (id TEXT, name TEXT, email TEXT, quota BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS user_metadata (id TEXT, name TEXT, email TEXT, quota BIGINT)")

      println("Dando de alta la información de usuarios")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 200000")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 200000")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 200000")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com', 200000")

    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
