package controllers

import javax.inject._

import acl.ACLConfig._
import controllers.ControllerImplicits._
import entities._
import services.{DBConnectionService}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._
import util.Secured
import play.api.libs.json.{Json, _}

import scala.concurrent.Future


class DBConnectionController @Inject()( dBConnectionService: DBConnectionService,
                                        secured: Secured) extends Controller {

  def createConnection(user_id: String) = secured.withAuth {
    Action.async(parse.json) {
      implicit request =>
        val connection = Json.fromJson[DBConnection](request.body)
        connection match {
          case JsSuccess(connection: DBConnection, _) => {
            dBConnectionService.saveConnection(user_id, connection) map { result =>
              result match {
                case Right(x) =>Status(200)(Json.obj("status" -> "Connection created success"))
                case Left(x) =>Status(409)(Json.obj("status" -> "Connection name already exist"))
              }

            }
          }
          case e: JsError => handleValidationError(e)
        }
    }
  }

  def getDBList(user_id: String, id: String) = secured.withUser(user_id) {
    Action.async { implicit  request =>
      dBConnectionService.getDBList(user_id, id) map { result =>
        result match {
          case Right(x) =>Status(200)(Json.obj("dbs"-> Json.toJson(x)))
          case Left(x) =>Status(500)(Json.obj("status"-> x))
        }
      }
    }
  }


  def getDBDetails(user_id: String, id: String) = secured.withUser(user_id) {
    Action.async(parse.json) { implicit  request =>
      val schema_name = (request.body \ "schema_name").as[String]
      dBConnectionService.getDBDetails(user_id, id, schema_name) map { result =>
        result match {
          case Right(x) =>Status(200)(Json.obj("schema_details"-> Json.toJson(x)))
          case Left(x) =>Status(500)(Json.obj("status"-> x))
        }
      }
    }
  }

  def runSqlQuery(user_id: String, id: String) = secured.withUser(user_id) {
    Action.async(parse.json) { implicit  request =>
      val query_meta = Json.fromJson[QueryMeta](request.body)
      query_meta match {
        case JsSuccess(query_meta: QueryMeta, _) => {
          dBConnectionService.runSqlQuery(user_id, id, query_meta) map { result =>
            result match {
              case Right(x) =>Status(200)(Json.obj("data"-> Json.toJson(x)))
              case Left(x) =>Status(500)(Json.obj("status"-> x))
            }

          }
        }
        case e: JsError => handleValidationError(e)
      }
    }
  }



  def getConnection(user_id: String,
                    page: Int, limit: Int) = secured.withUser(user_id) {
    Action.async {
      implicit request =>
        dBConnectionService.getConnection(user_id, page, limit) map { response =>
          Ok(if (response._2 == Nil) new JsArray(List())
          else Json.obj("total_count" -> response._1, "connections" -> Json.toJson(response._2)))
        }
    }
  }

  def getConnectionById(user_id: String,
                        id: String) =  secured.withUser(user_id) {
    Action.async { implicit request =>
      dBConnectionService.getConnectioById(user_id, id) map {
        case Some(x) => Ok(Json.toJson(x))
        case None => Ok(new JsObject(Map.empty))
      }
    }
  }

  def updateConnection(user_id: String,
                       id: String) = secured.withAuth {
    Action.async(parse.json) {
      implicit request =>
        val connection = Json.fromJson[DBConnection](request.body)
        connection match {
          case JsSuccess(connection: DBConnection, _) => {
            dBConnectionService.updateConnection(user_id, id, connection) map { result =>
              result match {
                case Right(x) =>Status(200)(Json.obj("status" -> "Connection updated success"))
                case Left(x) =>Status(409)(Json.obj("status" -> "Connection name already exist"))
              }
            }
          }
          case e: JsError => handleValidationError(e)
        }
    }
  }

  def deleteConnection(user_id: String,
                       id: String) = secured.withAuth {
    Action.async {
      implicit request =>
        dBConnectionService.deleteConnection(user_id, id) map { res =>
          Status(200)(Json.obj("status" -> "Connection deleted"))
        }
    }
  }

  def checkConnection(user_id: String) = secured.withAuth {
    Action.async(parse.json) {
      implicit request =>
        val connection = Json.fromJson[DBConnection](request.body)
        connection match {
          case JsSuccess(connection: DBConnection, _) => {
            dBConnectionService.checkConnection(user_id, connection) map { result =>
              result match {
                case Right(x) =>Status(200)(Json.obj("status"-> "Connection verified success"))
                case Left(x) =>Status(500)(Json.obj("status"-> x))
              }
            }
          }
          case e: JsError => handleValidationError(e)
        }
    }
  }

  def createFromQuery(user_id: String, id: String) = secured.withUser(user_id) {
    Action.async(parse.json) { implicit  request =>
      val query_meta = Json.fromJson[IngestRequest](request.body)
      query_meta match {
        case JsSuccess(query_meta: IngestRequest, _) => {
          dBConnectionService.createFromQuery(user_id, id, query_meta) map { result =>
            result match {
              case Right(x) =>Status(200)(Json.obj("data"-> Json.toJson(x)))
              case Left(x) =>Status(500)(Json.obj("status"-> x))
            }

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


}
