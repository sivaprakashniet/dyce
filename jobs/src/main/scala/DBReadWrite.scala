import java.sql.{Connection, DriverManager}

object DBReadWrite {
  val url = "jdbc:mysql://amplifyr.c6gsh1cdvu05.us-east-1.rds.amazonaws.com:3306/amplifyr_dev"
  val username = "amplifyr_master"
  val password = "idealabs"
  val job_table = "job"
  val job_id_col_name = "job_id"
  val job_request_col_name = "request_json"
  val job_response_col_name = "response_json"

  var connection: Connection = null

  def createDBConnection(): Connection = {
    connection = DriverManager.getConnection(url, username, password)
    connection
  }

  def executeUpdateQuery(db_conn: Connection, job_id: String, response: String) = {
    val statement = db_conn.createStatement()
    val query = "update " + job_table +
      " set " + job_response_col_name + " = '" + response +
      "'  where " + job_id_col_name + " = '" + job_id + "'"
    val result = statement.execute(query)
    result
  }

  def executeSelectQuery(db_conn: Connection, job_id: String) = {
    val statement = db_conn.createStatement()
    val query = "select "+ job_request_col_name +
      " from " + job_table +
      " where " + job_id_col_name + " = " + job_id
    val result = statement.executeQuery(query)
    if(result.next())
      result.getString(job_request_col_name)
    else
      "{}"
  }

}
