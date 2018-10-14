package controllers

import java.util.UUID
import javax.inject._
import java.net.URLDecoder

import play.api.mvc._
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import services.UserService
import _root_.util.Secured
import oauth.OAuth2
import entities.Acl
import ControllerImplicits.{aclReads, aclWrites}

class AuthenticationController @Inject()(userService: UserService,
                                         secured: Secured,
                                         oauth2: OAuth2) extends Controller {

  def index = Action { implicit request =>
    val callbackUrl = routes.AuthenticationController.callback(None, None).absoluteURL()
    val successURL = routes.UserController.getUser().absoluteURL()
    val state = UUID.randomUUID().toString
    val redirectUrl = oauth2.getAuthorizationUrl(callbackUrl, state)
    Ok(views.html.index("Index", redirectUrl))
      .withSession("oauth-state" -> state,
        "client-uri" -> URLDecoder.decode(successURL, "UTF-8"))
  }

  def loginWithGoogle(clientURI: String) = Action { implicit request =>
    val callbackUrl = routes.AuthenticationController.callback(None, None).absoluteURL()
    val state = UUID.randomUUID().toString
    val redirectUrl = oauth2.getAuthorizationUrl(callbackUrl, state)
    Redirect(redirectUrl, 302)
      .withSession("oauth-state" -> state,
        "client-uri" -> URLDecoder.decode(clientURI, "UTF-8"))
  }

  def callback(codeOpt: Option[String] = None,
               stateOpt: Option[String] = None) = Action.async { implicit request =>
    (for {
      code <- codeOpt
      state <- stateOpt
      oauthState <- request.session.get("oauth-state")
    } yield {
      if (state == oauthState) {
        Thread.sleep(300)
        oauth2.getToken(code).map { accessToken =>
          Redirect(routes.AuthenticationController
            .authenticateUser(accessToken, request.session.get("client-uri").get))
        }.recover {
          case ex: IllegalStateException => Unauthorized(ex.getMessage)
        }
      }
      else {
        Future.successful(BadRequest("Invalid google login"))
      }
    }).getOrElse(Future.successful(BadRequest("No parameters supplied")))
  }

  def authenticateUser(authToken: String,
                       url: String) = Action.async { implicit request =>
    Thread.sleep(300)
    oauth2.userInfo(authToken) flatMap (res => {
      val email = (res.json \ "email").asOpt[String]
      if (email != None) {
        val x = (res.json \ "picture").asOpt[String].get
        userService.authenticate(email.get.toString) flatMap (user => {
          if (user == Nil) Future { Redirect(url + "?status=403")}
          else {
            val user_object = user(0).copy(profile_image = Some(x.toString()))
            userService.updateUser(user_object.id.get,user_object) map { res =>
              Redirect(url + "?status=200&id=" + user(0).id.get + "&token=" + authToken)
            }
          }
        })
      } else {
        Future {
          Redirect(url + "?status=401")
        }
      }
    })
  }

  def userInfo = Action.async { request =>
    request.headers.get("Authorization")
      .fold(Future.successful(Unauthorized("401"))) { authToken =>
        oauth2.userInfo(authToken) map (res => {
          Ok(Json.obj("status" -> res.body))
        })
      }
  }

  def logout() = secured.withAuth {
    Action.async { request =>
      request.headers.get("Authorization")
        .fold(Future.successful(Unauthorized("401"))) { authToken =>
          oauth2.logout(authToken) map (res => {
            Ok(Json.obj("message" -> res.body))
          })
        }
    }
  }

  def setACLPermission(user_id: String) = secured.withUser(user_id) {
    Action.async { implicit request =>
      val bodyAsJson = request.body.asJson.getOrElse(new JsObject(Map.empty))
      val aclJsResult = Json.fromJson[Acl](bodyAsJson)
      aclJsResult match {
        case JsSuccess(acl: Acl, _) => userService.setACLPermission(acl) map { results =>
          Ok(if (results == Nil) new JsArray(List()) else Json.toJson(results))
        }
        case e: JsError => handleValidationError(e)
      }
    }
  }

  private def handleValidationError(e: JsError): Future[Result] =
    Future.successful(
      Status(400)(
        Json.obj("status"->"KO", "message" -> JsString(e.toString)))
    )

}