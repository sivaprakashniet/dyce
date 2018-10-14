package services

import javax.inject._

import entities._

import scala.concurrent.Future
import dao.DBConnectionDAO

import scala.concurrent.ExecutionContext.Implicits._
import job.JobService
import java.sql.{Connection, DriverManager, ResultSet, SQLException}

import scala.util.Try
import util.InferSchema


class DBConnectionService @Inject()(dBConnectionDAO: DBConnectionDAO,
                                    inferSchema: InferSchema,
                                    jobService: JobService) {

  type URL = String

  def uuid: String = java.util.UUID.randomUUID.toString

  def currentDateTime: Long = System.currentTimeMillis()

  def checkConnectionAndSave(user_id: String,
                             connection: DBConnection): Future[Either[Int, Int]] = {
    dBConnectionDAO.isExist(user_id, connection) flatMap { res =>
      if (res) Future {Left(409)}
      else dBConnectionDAO.create(connection) map (x => Right(x))
    }
  }

  def saveConnection(user_id: String,
                     connection: DBConnection): Future[Either[Int, Int]] = {

    val port = if (connection.port == None) Some(3306) else connection.port
    val db_connection = connection.copy(id = Some(uuid),
      user_id = Some(user_id), created_date = Some(currentDateTime),
      modified_date = Some(currentDateTime), port = port)
    checkConnectionAndSave(user_id, db_connection)
  }

  def getConnectioById(user_id: String,
                       id: String): Future[Option[DBConnection]] = {
    dBConnectionDAO.getConnectioById(user_id, id)
  }

  def getConnection(user_id: String, page: Int,
                    limit: Int): Future[(Int, Seq[DBConnection])] = {

    dBConnectionDAO.getConnectionCount(user_id) flatMap { count =>
      getConnectionWithCount(count, user_id, page, limit)
    }
  }

  def getConnectionWithCount(count: Int, user_id: String, page: Int,
                             limit: Int): Future[(Int, Seq[DBConnection])] = {
    dBConnectionDAO.getConnections(user_id, page, limit) map { result =>
      (count, result)
    }
  }

  def checkConnectionAnUpdate(user_id: String, id: String,
                              connection: DBConnection): Future[Either[Int, Int]] = {
    dBConnectionDAO.isExist(user_id, connection) flatMap { res =>
      if (res) Future {
        Left(409)
      }
      else {
        dBConnectionDAO.updateConnection(id, connection) map (x => Right(x.toInt))
      }
    }
  }

  def updateConnection(user_id: String, id: String,
                       connection: DBConnection): Future[Either[Int, Int]] = {
    val db_connection = connection.copy(id = Some(id), user_id = Some(user_id),
      created_date = Some(currentDateTime), modified_date = Some(currentDateTime))

    checkConnectionAnUpdate(user_id, id, db_connection)
  }

  def deleteConnection(user_id: String, id: String): Future[Int] = {
    dBConnectionDAO.deleteConnection(id)
  }

  def constructSQLURI(param: DBConnection): URL =
    "jdbc:" + param.connection_db_type.get + "://" + param.host_name + "/" +
      param.default_schema.getOrElse("")


  def checkConnection(user_id: String,
                      dbconnection: DBConnection): Future[Either[String, Connection]] = Future {

    val url = constructSQLURI(dbconnection)
    val response = Try {
      val connection: Connection = DriverManager.getConnection(url,
        dbconnection.username, dbconnection.password)
      Right(connection)
    }
    (response recover { case e: Exception => Left(e.getLocalizedMessage)
    }).get
  }


  //Get Database List
  def runQueryDBList(conn: Connection,
                     db: DBConnection): Future[Either[String, List[String]]] = Future {

    val dbs = conn.getMetaData().getCatalogs()
    var db_list: List[String] = List()
    while (dbs.next()) {
      db_list = db_list :+ dbs.getString(1)
    }
    Right(db_list)
  }

  def getAndVerifyConnection(user_id: String,
                             db: DBConnection): Future[Either[String, List[String]]] = {
    checkConnection(user_id, db) flatMap { result =>
      result match {
        case Left(x) => Future { Left(x) }
        case Right(connection) => runQueryDBList(connection, db)
      }
    }
  }

  def getDBList(user_id: String,
                connection_id: String): Future[Either[String, List[String]]] = {
    dBConnectionDAO.getConnectioById(user_id, connection_id) flatMap { conn =>
      if (conn == None) Future { Left("not found") }
      else getAndVerifyConnection(user_id, conn.get)
    }
  }

  def runQueryDBDetails(connection: Connection,
                        db_config: DBConnection): Future[Either[String, DBSchema]] = Future {

    val db = connection.getMetaData()
    val tables = db.getTables(null, db_config.default_schema.get, null, Array("TABLE"))
    var db_tables: List[TableInfo] = List()
    while (tables.next()) {
      val table_name = tables.getString(3)
      val schema_name = tables.getString(1)

      val columns = db.getColumns(null, schema_name, table_name, null)

      var db_columns: List[FieldSchema] = List()

      while (columns.next()) {
        db_columns = db_columns :+ new FieldSchema(
          columns.getString("COLUMN_NAME"),
          columns.getString("TYPE_NAME"),
          Some(if (columns.getString("COLUMN_SIZE") == null) 0
          else columns.getString("COLUMN_SIZE").toInt),
          Some(if (columns.getString("IS_NULLABLE") == "YES") true else false),
          Some(if (columns.getString("DECIMAL_DIGITS") == null) 0
          else columns.getString("DECIMAL_DIGITS").toInt),
          Some(columns.getString("REMARKS")),
          Some(if (columns.getString("IS_AUTOINCREMENT") == "YES") true
          else false))
      }
      db_tables = db_tables :+ new TableInfo(table_name, db_columns)
    }
    Right(new DBSchema(db_config, db_tables))
  }

  def runAndVerifyConnection(user_id: String,
                             db: DBConnection): Future[Either[String, DBSchema]] = {
    checkConnection(user_id, db) flatMap { result =>
      result match {
        case Left(x) => Future { Left(x) }
        case Right(connection) => runQueryDBDetails(connection, db)
      }
    }
  }

  def getDBDetails(user_id: String, connection_id: String,
                   schema_name: String): Future[Either[String, DBSchema]] = {
    dBConnectionDAO.getConnectioById(user_id, connection_id) flatMap { conn =>
      if (conn == None) Future { Left("not found") }
      else runAndVerifyConnection(user_id, conn.get
          .copy(default_schema = Some(schema_name)))
    }
  }

  def getColumnFromQuery(query: ResultSet):List[Field] = {
    var cols: List[Field] = List()
    if (query != null) {
      val columns = query.getMetaData()
      var i = 0
      while (i < columns.getColumnCount) {
        i += 1
        cols = cols :+ new Field(i, columns.getColumnName(i),
          columns.getColumnTypeName(i))
      }
    }
    cols
  }

  def getDataFromQuery(query: ResultSet,
                       cols: List[Field]): List[List[String]] = {
    var data: List[List[String]] = List()
    while (query.next()) {
      var row: List[String] = List()
      for (field <- cols) {
        row = row :+ query.getString(field.name)
      }
      data = data :+ row
    }
    data
  }

  def constructSchema(cols: List[Field]): List[ColumnMetadata] = {
      cols map (x =>
        new ColumnMetadata(None, x.name, None, x.position.toLong,
          inferSchema.getColType(x.data_type)._1,
          inferSchema.getColType(x.data_type)._2,
          inferSchema.getColType(x.data_type)._2, false,
          0,None,false,false,"","",None,None, None,None,None))
  }

  def runQuery(connection: Connection,
               query_meta: QueryMeta): Future[Either[Int, QueryResult]] = Future {

    val query = connection.createStatement()
    try {
      val rs = query.executeQuery(query_meta.query + " limit 0,10")
      val cols = getColumnFromQuery(rs)
      val data = getDataFromQuery(rs, cols)
      val schema = constructSchema(cols)
      Right(new QueryResult(cols, data, schema))
    } catch {
      case e: SQLException => {
        println(e.printStackTrace())
        Left(500)
      }
    } finally {
      if (query != null) query.close()
      if (connection != null) connection.close()
    }
  }

  def runQueryAndVerifyConnection(user_id: String, query_meta: QueryMeta,
                                  connection: DBConnection): Future[Either[Int, QueryResult]] = {
    checkConnection(user_id, connection) flatMap { result =>
      result match {
        case Left(x) => Future { Left(401) }
        case Right(connection) => runQuery(connection, query_meta)
      }
    }
  }

  def runSqlQuery(user_id: String, connection_id: String,
                  query_meta: QueryMeta): Future[Either[Int, QueryResult]] = {

    dBConnectionDAO.getConnectioById(user_id, connection_id) flatMap { conn =>
      if (conn == None) Future { Left(404) }
      else runQueryAndVerifyConnection(user_id, query_meta, conn.get
          .copy(default_schema = Some(query_meta.schema_name)))

    }
  }

  def createFromQuery(user_id: String, connection_id: String,
                    meta: IngestRequest):Future[Either[Int, String]] = {
    dBConnectionDAO.getConnectioById(user_id, connection_id) flatMap { conn =>
      if (conn == None) Future { Left(404) }
      else{
        val db_connection = conn.get.copy(default_schema = Some(meta.query_meta.schema_name))
        val params = new IngestParameters(meta.query_meta.query,meta.query_meta.name,
          db_connection, meta.tables, meta.columns)
          jobService.createFromQueryJob(user_id, params) map{ res =>
            Right(res)
          }
      }
    }
  }



}
