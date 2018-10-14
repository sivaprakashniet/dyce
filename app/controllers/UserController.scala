package controllers

import javax.inject._

import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.json._
import _root_.util.Secured
import services.{SampleDatasetService, UserService}
import ControllerImplicits._
import acl.ACLConfig._
import entities.User

class UserController @Inject()(userService: UserService, sampleDatasetService: SampleDatasetService,
                               secured: Secured)(implicit exec: ExecutionContext) extends
                                Controller {

  def getUser = secured.withAuth {
    Action.async { implicit request =>
      userService.getusers map { users =>
        Ok(if (users == Nil) new JsArray(List()) else Json.toJson(users))
      }
    }
  }

  def findById(user_id: String) = secured.withAuth {
    Action.async { implicit request =>
      userService.findById(user_id) map {
        case Some(x) => Ok(Json.toJson(x))
        case None => Ok(new JsObject(Map.empty))
      }
    }
  }

  def createUser = secured.withAuth {
    Action.async(parse.json) {
      implicit request =>
        val user_meta = Json.fromJson[User](request.body)
        user_meta match {
          case JsSuccess(u: User, _) => {
            sampleDatasetService.createUser(u) map { result =>
              Status(200)(Json.obj("status" -> "User created success"))
            }
          }
          case e: JsError => handleValidationError(e)
        }
    }
  }

  private def handleValidationError(e: JsError): Future[Result] =
    Future.successful(
      Status(400)(
        Json.obj("errors" -> JsError.toJson(e).toString()))

    )

  // For Testing ACL perminssions
  def sampleRead(user_id: String, resource_id: String) =
   secured.withProtectedAcl(user_id, resource_id,
     DatasetResource, ReadPrivilege) {
      Action { implicit request =>
        Ok("success")
      }
    }

  def sampleUpdate(user_id: String, resource_id: String) =
    secured.withProtectedAcl(user_id, resource_id,
      DatasetResource, UpdatePrivilege) {
      Action { implicit request =>
        Ok("success")
      }
    }

  def sampleDelete(user_id: String, resource_id: String) =
    secured.withProtectedAcl(user_id, resource_id,
      DatasetResource, DeletePrivilege) {
      Action { implicit request =>
        Ok("success")
      }
    }
}