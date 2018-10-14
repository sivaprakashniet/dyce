package controllers

import javax.inject.Inject

import _root_.util.Secured
import acl.ACLConfig._
import controllers.ControllerImplicits._
import entities._
import filesystem.FilesystemService
import job.JobService
import play.api.libs.json._
import play.api.mvc.{Action, Controller, Result}
import services.FileService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


class FileHandlerController @Inject()(fileService: FileService,
                                      filesystemService: FilesystemService,
                                      secured: Secured,
                                      jobService: JobService)(implicit exec: ExecutionContext) extends Controller {

  def getFileMetadataSecured(user_id: String,
                             page: Int, limit: Int,
                             q: String, sort_key:String,
                             sort_type:String) = secured.withUser(user_id) {
    getFileMetadata(user_id, page, limit, q, sort_key, sort_type)
  }

  def getFileMetadata(user_id: String, page: Int,
                      limit: Int, q: String,
                      sort_key:String, sort_type:String) = Action.async { implicit request =>
    fileService.getFileMetadata(user_id, page, limit, q, sort_key, sort_type) map { response =>
      Ok(if (response._2 == Nil) new JsArray(List())
      else Json.obj("total_count"->response._1, "datasets" ->Json.toJson(response._2)))
    }
  }

  def getFileMetadataByIdSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource,
      ReadPrivilege) {
      getFileMetadataById(user_id, dataset_id)
    }

  def getFileMetadataById(user_id: String,
                          dataset_id: String) = Action.async { implicit request =>
    fileService.getFileMetadataById(dataset_id) map {
      case Some(x) => Ok(Json.toJson(x))
      case None => Ok(new JsObject(Map.empty))
    }
  }

  def CheckDatasetName(user_id: String) = Action.async(parse.json) { implicit request =>
      val json = request.body
    val json_to_map = Json.fromJson[CheckDuplicateName](request.body)
    json_to_map match {
      case JsSuccess(u, path) => {
        fileService.CheckDuplicateName(user_id, u) map { result =>
          if(result)
            Status(409)(Json.obj("status" -> "Dataset name already exist"))
          else
            Status(200)(Json.obj("status" -> "Metadata for columns saved successfully"))
        }
      }
      case e: JsError => handleValidationError(e)
    }
  }


  def getPresignedUrlForUploadSecured(user_id: String) = secured.withUser(user_id) {
    getPresignedUrlForUpload(user_id)
  }

  def getPresignedUrlForUpload(user_id: String) = Action.async(parse.json) { implicit request =>
    val json = request.body
    val file_path = (json \ "file_path").as[String]
    filesystemService.generatePresignedUrlForUpload(user_id, file_path) map { result =>
      val response = Try {
        result match {
          case Right(x) => {
            Status(200)(Json.obj("presignedurl" -> x))
          }
          case Left(y) => {
            Status(y("error code").toInt)(Json.obj("error" -> y))
          }
        }
      }

      (response recover {
        case e: Exception =>
          Status(500)(Json.obj("error" -> "Error while creating response"))
      }).get

    }
  }

  def getPresignedUrlForDownloadSecured(user_id: String,
                                        dataset_id: String) = secured.withUser(user_id) {
    getPresignedUrlForDownload(user_id, dataset_id)
  }


  def downloadDatasetSecured(user_id: String, dataset_id: String) = Action.async { implicit request =>
    jobService.downloadDataset(user_id, dataset_id) map { res =>
      Status(200)(Json.obj("status" -> "Download dataset job started"))
    }
  }

  def getPresignedUrlForDownload(user_id: String,
                                 dataset_id: String) = Action.async { implicit request =>
    val json = request.body
    filesystemService.generatePresignedUrlForDownload(user_id, dataset_id) map { result =>
      val response = Try {
        result match {
          case Right(x) => {
            Status(200)(Json.obj("presignedurl" -> x))
          }
          case Left(y) => {
            Status(y("error code").toInt)(Json.obj("error" -> y))
          }
        }
      }

      (response recover {
        case e: Exception =>
          Status(500)(Json.obj("error" -> "Error while creating response"))
      }).get

    }
  }


  def saveFileMetadataSecured(user_id: String) = secured.withUser(user_id) {
    saveFileMetadata(user_id)
  }

  def saveFileMetadata(user_id: String) =
    Action.async(parse.json) { implicit request =>
      val file_metadata_result = Json.fromJson[FileMetadata](request.body)

      file_metadata_result match {
        case JsSuccess(file_metadata, path) => {
          fileService.saveFileMetadata(user_id, file_metadata) map { result =>
            if(result.isRight)
              Status(200)(Json.obj(
                "status" -> "File metadata saved successfully",
                "dataset_id" -> result.right.get)
              )
            else
              Status(409)(Json.obj(
                "status" -> "Dataset name already exist")
              )

          }
        }
        case e: JsError => handleValidationError(e)
      }

    }

  def getDatasetStatusSecured(user_id: String) =
    secured.withUser(user_id) {
      getDatasetStatus(user_id)
    }

  def getDatasetStatus(user_id: String) =
    Action.async(parse.json) { implicit request =>
      val json = request.body
      val file_path = (json \ "file_path").as[String]
      val f = fileService.getFilemetadataStatus(user_id, 0.toString, file_path)
      f map { status =>
        if (status) Status(409) else Ok(Json.obj("message" -> "success"))
      }
    }

  def deleteFileMetadataSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id,
      DatasetResource, UpdatePrivilege) {
      deleteFileMetadata(user_id, dataset_id)
    }

  def deleteFileMetadata(user_id: String, dataset_id: String) =
    Action.async { implicit request =>
      fileService.deleteFileMetadata(user_id, dataset_id) map {
        case x => Ok(Json.obj("message" -> s"FileMetadata deleted successfully"))
        case _ => InternalServerError(
          Json.obj("message" -> "Error")
        )
      } recover {
        case ex => InternalServerError(
          Json.obj("message" -> JsString(ex.getMessage))
        )
      }
    }

  def updateFileMetadataSecured(user_id: String,
                                dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, UpdatePrivilege) {
      updateFileMetadata(user_id, dataset_id)
    }

  def updateFileMetadata(user_id: String,
                         dataset_id: String) = Action.async { implicit request =>
    val bodyAsJson = request.body.asJson.getOrElse(new JsObject(Map.empty))
    val filemetaJsResult = Json.fromJson[FileMetadata](bodyAsJson)

    filemetaJsResult match {
      case JsSuccess(f: FileMetadata, _) => {
        fileService.checkAndUpdateDataset(user_id, dataset_id, f) map {
          case x if (x > 0) => Ok(Json.obj("message" -> s"FileMetadata updated successfully"))
          case _ => InternalServerError(
            Json.obj("message" -> JsString("Error"))
          )
        } recover {
          case ex if (ex.getMessage == "Duplicate") => Status(409)(
            Json.obj("message" -> JsString(ex.getMessage))
          )
          case ex => InternalServerError(
            Json.obj("message" -> JsString(ex.getMessage))
          )
        }
      }
      case e: JsError => handleValidationError(e)
    }
  }

  private def handleValidationError(e: JsError): Future[Result] =
    Future.successful(
      Status(400)(
        Json.obj("errors" -> JsError.toJson(e).toString()))
    )

  def saveColumnMetadataSecured(user_id: String,
                                dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, CreatePrivilege) {
      saveColumnMetadata(user_id, dataset_id)
    }

  def saveColumnMetadata(user_id: String, dataset_id: String) =
    Action.async(parse.json) { implicit request =>
      val column_metadatas_result = Json.fromJson[SchemaList](request.body)

      column_metadatas_result match {
        case JsSuccess(column_metadatas, path) => {
          fileService.saveColumnMetadata(user_id, dataset_id,
            column_metadatas.schema_list) map { result =>
            Status(200)(Json.obj("status" -> "Metadata for columns saved successfully"))
          }
        }
        case e: JsError => handleValidationError(e)
      }

    }

  def updateDatasetSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, UpdatePrivilege) {
      updateDataset(user_id, dataset_id)
    }

  def updateDataset(user_id: String, dataset_id: String) =
    Action.async(parse.json) { implicit request =>
      val column_metadatas_result = Json.fromJson[SchemaList](request.body)
      column_metadatas_result match {
        case JsSuccess(column_metadatas, path) => {
          jobService.getDatasetAndupdateOrAddColumns(user_id, dataset_id,
            column_metadatas) map { result =>
            Status(200)(Json.obj("status" -> "Column metadata for columns updated successfully"))
          }
        }
        case e: JsError => handleValidationError(e)
      }
    }

  def numericBinning(user_id: String, dataset_id: String) = Action.async(parse.json) { implicit request =>
    val column_bin = Json.fromJson[List[ColumnBins]](request.body)
    column_bin match {
      case JsSuccess(column_bin, path) => {
        jobService.numericBinColumn(user_id, dataset_id,
          column_bin) map { result =>
          Status(200)(Json.obj("status" -> "Numberic binning job started success"))
        }
      }
      case e: JsError => handleValidationError(e) //
    }
  }

  def updateColumnMetadataSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, UpdatePrivilege) {
      updateColumnMetadata(dataset_id)
    }

  def updateColumnMetadata(dataset_id: String) =
    Action.async(parse.json) { implicit request =>
      val column_metadatas_result = Json.fromJson[SchemaList](request.body)

      column_metadatas_result match {
        case JsSuccess(column_metadatas, path) => {
          fileService.updateColumnMetadata(dataset_id,
            column_metadatas.schema_list) map { result =>
            Status(200)(Json.obj("status" -> "Metadata for columns updated successfully"))
          }
        }
        case e: JsError => handleValidationError(e)
      }
    }

  def generateMetadataSecured(user_id: String) = secured.withUser(user_id) {
    generateMetadata(user_id)
  }

  def generateMetadata(user_id: String) = Action.async(parse.json) { implicit request =>
    val preview_data_and_schema_result = Json.fromJson[PreviewData](request.body)

    preview_data_and_schema_result match {
      case JsSuccess(preview_data_and_schema, path) => {
        fileService.generateMetadata(preview_data_and_schema) map {
          result =>
            Status(200)(Json.obj("schema_list" -> Json.toJson(result)))
        }
      }
      case e: JsError => handleValidationError(e)

    }
  }

  def getColumnMetadataSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      getColumnMetadata(dataset_id)
    }

  def getColumnMetadata(dataset_id: String) = Action.async { implicit request =>
    fileService.getColumnMetadata(dataset_id) map { result =>
      Status(200)(Json.obj("dataset_id" -> dataset_id, "schema_list" -> Json.toJson(result)))
    }
  }

  def viewDatasetSecured(user_id: String, dataset_id: String,
                         page: Int, limit: Int) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      viewDataset(user_id, dataset_id, page, limit)
    }

  def viewDataset(user_id: String, dataset_id: String,
                  page: Int, limit: Int) = Action.async { implicit request =>
      fileService.readFile(user_id, dataset_id, limit, page) map { res =>
        Status(200)(Json.toJson(res))
      }
    }
}