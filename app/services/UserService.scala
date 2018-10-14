package services


import javax.inject._

import dao.{UserDAO, AclDAO}
import entities.{User, Acl}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

class UserService @Inject()(userDAO: UserDAO,
                            aclDAO: AclDAO) {
  def getusers: Future[Seq[User]] = {
    userDAO.getUser
  }

  def findById(userId: String): Future[Option[User]] = {
    userDAO.findById(userId)
  }

  def authenticate(email: String): Future[Seq[User]] = {
    userDAO.authenticate(email)
  }

  def updateUser(user_id: String, user: User): Future[Long] = {
    userDAO.updateUser(user_id, user)
  }


  def authorization(email: String, id: String): Future[Option[User]] = {
    userDAO.authorization(email, id)
  }

  def setACLPermission(acl: Acl): Future[Acl] = {
    aclDAO.deleteACL(acl) flatMap (res => {
      aclDAO.createACL(acl)
    })
  }

  def withProtectedAcl(user_id: String, object_id:
  String, resource_type: String,
                       privilege: String): Future[Seq[Acl]] = {
    aclDAO.getUserObjectPermissionDeny(user_id, object_id, resource_type,
                                                      privilege) flatMap {
      case Nil => aclDAO.getUserObjectPermissionAllow(user_id, object_id,
        resource_type, privilege)
      case _ => Future.successful(Seq.empty[Acl])
    }
  }
}