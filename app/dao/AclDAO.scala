package dao

import javax.inject.Inject
import entities.{Acl}
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile
import scala.concurrent.Future

import scala.concurrent.ExecutionContext.Implicits.global

class AclDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.get[JdbcProfile]
  def createUUID: String = java.util.UUID.randomUUID.toString

  import dbConfig._
  import driver.api._

  class UserObjectPermissionTable(tag: Tag)
    extends Table[Acl](tag, "acl") {
    def id = column[Option[String]]("id", O.PrimaryKey)

    def user_id = column[String]("user_id")

    def object_id = column[String]("object_id")

    def resource_type = column[String]("resource_type")

    def privilege = column[String]("privilege")

    def permission = column[String]("permission")

    override def * =
      (id, user_id, object_id, resource_type,
        privilege, permission) <> ((Acl.apply _).tupled, Acl.unapply)
  }

  implicit val userObjectPermissions = TableQuery[UserObjectPermissionTable]

  def createACL(acl: Acl): Future[Acl] = {
    val acl_object = acl.copy(id = Some(createUUID))
    db.run(userObjectPermissions += acl_object) map { result =>
      acl
    }
  }

  def deleteACL(aclObject: Acl): Future[Int] = {
    db.run(
      userObjectPermissions
        .filter(_.user_id === aclObject.user_id)
        .filter(_.object_id === aclObject.object_id)
        .filter(_.resource_type === aclObject.resource_type)
        .delete
    )
  }

  def getUserObjectPermissionAllow(user_id: String,
                                   object_id: String,resource_type: String,
                                   privilege: String): Future[Seq[Acl]] = {
    db.run(userObjectPermissions
      .filter(x =>
        x.user_id === user_id && x.object_id === object_id &&
          x.resource_type === resource_type &&
          (x.privilege === "*" || x.privilege === privilege)
          && x.permission === "allow")
      .result
    )
  }

  def getUserObjectPermissionDeny(user_id: String,
                                  object_id: String, resource_type: String,
                                  privilege: String): Future[Seq[Acl]] = {
    db.run(userObjectPermissions
      .filter(x =>
        x.user_id === user_id && x.object_id === object_id &&
          x.privilege === privilege && x.permission === "deny"
      )
      .result
    )
  }

  def deleteACLPermission(object_id: String): Future[Int] = {
    db.run(userObjectPermissions
        .filter(x => x.object_id === object_id)
        .delete
    )
  }

}
