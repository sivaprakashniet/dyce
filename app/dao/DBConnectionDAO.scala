package dao

import javax.inject.Inject

import entities.{DBConnection}
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class DBConnectionDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {



  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class DBConnectionTable(tag: Tag)
    extends Table[DBConnection](tag, "db_connection") {

    def id = column[Option[String]]("id", O.PrimaryKey)

    def connection_name = column[String]("connection_name")

    def connection_method = column[String]("connection_method")

    def host_name = column[String]("host_name")

    def port = column[Option[Int]]("port")

    def username = column[String]("username")

    def password = column[String]("password")

    def default_schema = column[Option[String]]("default_schema")

    def user_id = column[Option[String]]("user_id")

    def connection_db_type = column[Option[String]]("connection_db_type")

    def created_date = column[Option[Long]]("created_date")

    def modified_date = column[Option[Long]]("modified_date")



    override def * =
      (id, connection_name,connection_method, host_name, port,username, password,
        default_schema, user_id, connection_db_type, created_date,
        modified_date) <> ((DBConnection.apply _).tupled, DBConnection.unapply)
  }

  implicit val connections = TableQuery[DBConnectionTable]

  def current_date_time: Long = System.currentTimeMillis()


  def create(connection: DBConnection): Future[Int] = {
    db.run(connections += connection) map (_.toInt)
  }


  def getConnectioById(user_id: String,
                       id: String):Future[Option[DBConnection]] = {
    db.run(connections.filter(_.user_id === user_id)
        .filter(_.id === id).result.headOption)
  }


  def updateConnection(id: String, connection: DBConnection): Future[Long] = {
    db.run(connections.filter(_.id === id)
      .update(connection.copy(modified_date = Some(current_date_time)))
      .map(_.toLong))
  }

  def deleteConnection(id: String): Future[Int] = {
    db.run(connections.filter(_.id === id).delete)
  }

  def getConnections(user_id: String, page: Int,
                     limit: Int): Future[Seq[DBConnection]] = {

    val offset = (limit * (page - 1))-(page - 1)+(page-1)
    db.run(connections.filter(_.user_id === user_id)
      .sortBy(_.id.desc).drop(offset).take(limit).result)
  }

  def getConnectionCount(user_id: String): Future[Int] = {
    db.run(connections.filter(_.user_id === user_id).result) map { q =>q.length}
  }

  def isExist(user_id: String,
              connection: DBConnection): Future[Boolean] = {

    if(connection.id == None)
      db.run(connections.filter(_.user_id === user_id)
        .filter(_.connection_name === connection.connection_name).exists.result)
    else
      db.run(connections.filter(_.user_id === user_id)
        .filter(_.connection_name === connection.connection_name)
        .filter(_.id =!= connection.id.get).exists.result)
  }

}
