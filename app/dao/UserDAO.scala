package dao

import javax.inject.Inject
import entities.{User}
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class UserDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class UserTable(tag: Tag)
    extends Table[User](tag, "user") {
    def id = column[Option[String]]("id", O.PrimaryKey)

    def first_name = column[String]("first_name")

    def last_name = column[String]("last_name")

    def username = column[String]("username")

    def email = column[String]("email")

    def created_date = column[Option[Long]]("created_date")

    def modified_date = column[Option[Long]]("modified_date")

    def profile_image = column[Option[String]]("profile_image")

    def role = column[String]("role")

    override def * =
      (id, first_name, last_name, username, email,
        created_date, modified_date, profile_image, role) <> ((User.apply _).tupled, User.unapply)
  }

  implicit val users = TableQuery[UserTable]


  def create(user: User): Future[User] = {
    db.run(users += user) map { x =>
      user
    }
  }

  def getUser: Future[Seq[User]] = {
    db.run(users.result)
  }

  def findById(user_id: String): Future[Option[User]] = {
    db.run(users.filter(_.id === user_id).result.headOption)
  }

  def authenticate(email: String): Future[Seq[User]] = {
    db.run(users.filter(_.email === email).result)
  }

  def updateUser(user_id: String, user: User): Future[Long] = {
    db.run(users.filter(_.id === user_id).update(user).map(_.toLong))
  }

  def authorization(email: String, user_id: String): Future[Option[User]] = {
    db.run(
      users.filter(_.email === email).filter(_.id === user_id).result.headOption
    )
  }
}
