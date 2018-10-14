package util

import javax.inject.Inject
import play.api.mvc.{Action}
import scala.concurrent.{Future}
import play.api.mvc.Results.{Status}
import scala.concurrent.ExecutionContext.Implicits.global
import oauth.OAuth2
import services.UserService

class Secured @Inject()(oauth2: OAuth2, userservice: UserService) {

  def withAuth[A](action: Action[A]): Action[A] = Action.async(action.parser) { request =>
    try {
      val token = request.headers.get("Authorization").get
      oauth2.tokenInfo(token).flatMap { res =>
        if (res.isLeft) Future {
          Status(401)("{ 'error': '" + res.left.get + "'}")
        }
        else {
          val token_res = res.right.get
          val user_email = (token_res.json \ "email").asOpt[String].getOrElse()
          if (user_email == None) Future {
            Status(401)("{ 'error': 'Invalid user'}")
          }
          else {
            userservice.authenticate(user_email.toString).flatMap(user => {
              if (user != Nil) action(request) else Future {
                Status(403)("{ 'error': 'Your email (" + user_email + ") is not provisioned'}")
              }
            })
          }
        }
      }
    } catch {
      case e: Exception => Future {
        Status(401)("{ 'error': 'Authorization token not found'}").withHeaders("WWW-Authenticate" -> "false")
      }
    }
  }

  def withUser[A](user_id: String)(action: Action[A]): Action[A] = Action.async(action.parser) { request =>
    try {
      val token = request.headers.get("Authorization").get
      oauth2.tokenInfo(token).flatMap { res =>
        if (res.isLeft) Future {
          Status(401)("{ 'error': '" + res.left.get + "'}")
        }
        else {
          val token_res = res.right.get
          val user_email = (token_res.json \ "email").asOpt[String].getOrElse()
          if (user_email == None) Future {
            Status(401)("{ 'error': 'Invalid user'}")
          }
          else {
            userservice.authenticate(user_email.toString).flatMap(u => {
              if (u != Nil)
                userservice.authorization(user_email.toString, user_id)
                  .flatMap(user => {
                    if (user != None) action(request) else Future {
                      Status(403)("{'error': 'Token and User Id mismatch'}")
                    }
                  })
              else
                Future {
                  Status(403)("{ 'error': 'Your email (" + user_email + ") is not provisioned'}")
                }
            })
          }
        }
      }
    } catch {
      case e: Exception => Future {
        Status(401)("{ 'error': 'Authorization token not found'}")
          .withHeaders("WWW-Authenticate" -> "false")
      }
    }
  }

  def withProtectedAcl[A]
  (user_id: String, object_id: String, resource_type: String, privilege: String)
  (action: Action[A]): Action[A] = Action.async(action.parser) { request =>
    try {
      val token = request.headers.get("Authorization").get
      oauth2.tokenInfo(token).flatMap { res =>
        if (res.isLeft) Future {
          Status(401)("{ 'error': '" + res.left.get + "'}")
        }
        else {
          val token_res = res.right.get
          val user_email = (token_res.json \ "email").asOpt[String].getOrElse()
          if (user_email == None) Future {
            Status(401)("{ 'error': 'Invalid user'}")
          }
          else {
            userservice.authenticate(user_email.toString).flatMap(u => {
              if (u != Nil)
                userservice.authorization(user_email.toString, user_id).flatMap(user => {
                  if (user != None) {
                    val acl = userservice.withProtectedAcl(user_id, object_id, resource_type, privilege)
                    acl flatMap { status =>
                      if(status != Nil) action(request)
                      else Future {
                        Status(403)("{'error': 'Permission denied'}")
                      }
                    }
                  }
                  else Future {
                    Status(403)("{'error': 'Accesss Token for (" + user_email + ")and User Id mismatch'}")
                  }
                })
              else
                Future {
                  Status(403)("{ 'error': 'Your email (" + user_email + ") is not provisioned'}")
                }
            })
          }
        }
      }
    } catch {
      case e: Exception => Future {
        Status(401)("{ 'error': 'Authorization token not found'}")
          .withHeaders("WWW-Authenticate" -> "false")
      }
    }
  }


}