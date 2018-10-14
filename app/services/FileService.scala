package services

import javax.inject._

import actors.MasterActor
import actors.MasterActor.{CustomJobDAG, JobDAG}
import akka.actor.{ActorSystem, Props}
import controllers.ControllerImplicits._
import dao._
import entities._
import filesystem.{FilesystemService, S3Config}
import job.{JobResult, PipelineService}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import play.api.Configuration
import play.api.libs.json.Json
import services.SparkConfig._
import util.InferSchema

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FileService @Inject()(userService: UserService,
                            filesystemService: FilesystemService,
                            s3Config: S3Config,
                            configuration: Configuration,
                            pipelineService:PipelineService,
                            jobResult: JobResult,
                            fileMetadataDAO: FileMetadataDAO,
                            modelScoringService: ModelScoringService,
                            columnMetadataDAO: ColumnMetadataDAO,
                            aclDAO: AclDAO, inferSchema: InferSchema,
                            jobDAO: JobDAO, anomalyDAO: AnomalyDAO) {



  val _system = ActorSystem("Amplifyr-Master-Pipeline_"+ createUUID())
  val master = _system.actorOf(Props(classOf[MasterActor],s3Config,
    jobResult, filesystemService, configuration, pipelineService, fileMetadataDAO), "master")

  def generateMetadata(preview_data_and_schema: PreviewData) = Future {
    val data_preview = preview_data_and_schema.preview_data
    val schema_list = preview_data_and_schema.schema_list


    val inferred_schema_list = inferSchema.infer_schema(
      data_preview.filter(x => x.size == data_preview(0).size))

    (inferred_schema_list zip schema_list).map { case (t, s) =>
      (s.copy(datatype = t("datatype").toString, format = t("format").toString, display_format = t("format").toString, decimal = t("decimal").toString.toLong))
    }

  }
  def getFileMetaWithCount(user_id: String,
                           q: String, meta:Seq[FileMetadata]):Future[(Int, Seq[FileMetadata])] = {
    fileMetadataDAO.getFileMetadataCount(user_id, q) map { count =>
      (count, meta)
    }
  }
  def getFileMetadata(user_id: String, page: Int,
                      limit: Int, q: String,
                      sort_key:String, sort_type:String):Future[(Int, Seq[FileMetadata])] = {
    fileMetadataDAO.getFileMetadata(user_id, page, limit, q, sort_key, sort_type) flatMap { res =>
      getFileMetaWithCount(user_id,q, res)
    }
  }


  def saveFileMetadata(user_id: String, file_metadata: FileMetadata): Future[Either[Boolean, String]] = {
    val fileMetadata = file_metadata.copy(id = Some(createUUID()),
      created_date = Some(current_date), download_file_path = Some(file_metadata.file_path),
      updated_date = Some(current_date), user_id = Some(user_id),
      number_of_rows = Some(0), number_of_columns = Some(0),
      dataset_status = Some("UPLOADED"), decision_tree_count = Some(0),
      anomaly_count = Some(0), gbt_count = Some(0),
      random_forest_count = Some(0), model_count = Some(0),
      download_status = Some("FINISHED"))
    fileMetadataDAO.checkDuplicate(user_id, None, file_metadata.dataset_name) flatMap { res =>
      if (res) Future{ Left(false) }  else {
        fileMetadataDAO.saveFileMetadata(fileMetadata) flatMap { dataset_id =>
          userService.setACLPermission(Acl(None, user_id, dataset_id, "DatasetAPI", "*", "allow")) map {
            result =>
              Right(dataset_id)
          }
        }
      }
    }
  }

  def createUUID(): String = java.util.UUID.randomUUID.toString
  def currentDateTime = System.currentTimeMillis()
  def getFilemetadataStatus(dataset_id: String,
                            user_id: String, path: String): Future[Boolean] = {
    fileMetadataDAO.getFilemetadataStatus(dataset_id, user_id, path)
  }


  def checkAndUpdateDataset(user_id: String, dataset_id: String,
                            filemetadata: FileMetadata): Future[Int] = {
    fileMetadataDAO.getFilemetadataStatus(dataset_id, user_id, filemetadata.file_path) flatMap {
      case true => Future.successful(throw new Exception("Duplicate"))
      case false => getAndUpdateDataset(user_id, dataset_id, filemetadata)
    }
  }

  def getAndUpdateDataset(user_id: String, dataset_id: String,
                          new_metadata: FileMetadata): Future[Int] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap {
      case None => Future.successful(throw new Exception("Error"))
      case Some(x) => renameDatasetUpdateMetaData(user_id, dataset_id,
        x.file_path, new_metadata)
    }
  }

  def renameDatasetUpdateMetaData(user_id: String,
                                  dataset_id: String, source_path: String,
                                  new_metadata: FileMetadata): Future[Int] = {
    renameFile(source_path, new_metadata.file_path, user_id) flatMap {
      case true => fileMetadataDAO.updateFileMetadata(dataset_id, new_metadata.file_path,
        new_metadata.dataset_name, current_date)
      case false => Future.successful(throw new Exception("Error"))
    }
  }

  def renameFile(source_path: String,
                 destination_path: String, user_id: String): Future[Boolean] = Future {
    filesystemService.renameFileOrFolder(source_path,
      destination_path, user_id).getOrElse(false)
  }

  def current_date = getCurrentDate().toString

  def getCurrentDate() = System.currentTimeMillis()

  def deleteFileMetadata(user_id: String, id: String): Future[Int] = {
    fileMetadataDAO.getFileMetadataById(id) flatMap {
      case None => Future.successful(throw new Exception("Not found"))
      case Some(x) => deleteFileOrFolder(id, x.file_path, user_id)
    }
  }

  def deleteFileOrFolder(dataset_id: String,
                         file_path: String, user_id: String): Future[Int] = {
    filesystemService.deleteFileOrFolder(file_path, user_id) match {
      case _ => deleteMetadataAndColumnMetadata(dataset_id)
    }
  }

  def deleteModelByDataset(dataset_id: String): Future[Int] = {
    columnMetadataDAO.deleteColumnMetadataByDataset(dataset_id) flatMap { res =>
      modelScoringService.deleteByDatasetId(dataset_id)
    }
  }

  def deleteMetadataAndColumnMetadata(dataset_id: String): Future[Int] = {
    fileMetadataDAO.deleteFileMetadata(dataset_id) flatMap {
      case 0 => Future.successful(throw new Exception("Error"))
      case x => deleteModelByDataset(dataset_id)
    }
  }

  def saveColumnMetadata(user_id: String, dataset_id: String,
                         columns_details: List[ColumnMetadata]) = {

    val all_columns_details = columns_details map { case column_details =>
      column_details.copy(id = Some(createUUID()), dataset_id = Some(dataset_id),
        created_date = Some(current_date),
        modified_date = Some(current_date),
        status = Some("unmodified"))
    }
    columnMetadataDAO.saveColumnMetadata(all_columns_details) map { res =>
      // After saved columns trigger pipleine
      val result = master ! JobDAG("Pre-Processing", user_id, dataset_id, None)
    }
  }

  def updateAndAddColumn(dataset:FileMetadata, columns: List[ColumnMetadata],
                         response_filepath: String): Future[List[Int]] = {

    val deleted_columns = columns.filter(c => c.status.get == "deleted")

    filesystemService.readJson(response_filepath) flatMap { new_column_details =>

      val new_columns = Json.fromJson[List[ColumnMetadata]](Json.parse(new_column_details.get)).get
      val nc = new_columns.filter(c => c.status.get == "new")
      val newcolumns = (nc map { case col =>
        col.copy(id = Some(createUUID()), dataset_id = Some(dataset.id.get),
          created_date = Some(current_date),
          modified_date = Some(current_date), status = Some("unmodified"))
      })

      val mc = columns.filter(c => c.status.get == "modified")

      val modified_columns = (mc map { case col =>
        col.copy(name = col.new_name.get, dataset_id = Some(dataset.id.get),
          created_date = Some(current_date),
          modified_date = Some(current_date), status = Some("unmodified"))
      })

      val columns_to_compute = newcolumns ++ modified_columns

      columnMetadataDAO.updateColumnMetadata(modified_columns) flatMap { res =>
        manageColumnMeta(dataset, deleted_columns, newcolumns, columns_to_compute)
      }
    }


  }

  def manageColumnMeta(dataset: FileMetadata, deleted_columns: List[ColumnMetadata],
                       newcolumns: List[ColumnMetadata],
                       columns_to_compute: List[ColumnMetadata]): Future[List[Int]] = {
    columnMetadataDAO.saveColumnMetadata(newcolumns) flatMap { res =>
      deleteColumnMetaData(dataset, deleted_columns, columns_to_compute)
    }
  }

  def deleteColumnMetaData(dataset:FileMetadata, deleted_columns: List[ColumnMetadata],
                           columns_to_compute: List[ColumnMetadata]): Future[List[Int]] = {
      // trigger univariate job after updating columns

    if(columns_to_compute.length >0) {
      val result = master ! CustomJobDAG("Updated_columns_univariate", dataset, columns_to_compute)
    }
    val list_of_cols = deleted_columns map (x =>x.id.get)

    jobResult.removeColumnsSummary(dataset.id.get, list_of_cols) flatMap { res =>
      columnMetadataDAO.deleteColumnMetadata(deleted_columns)
    }
  }

  def updateColumnMetadata(dataset_id: String, columns_details: List[ColumnMetadata]) = {
    val all_columns_details = columns_details map { case column_details =>
      column_details.copy(dataset_id = Some(dataset_id),
        modified_date = Some(current_date), status = Some("unmodified"))
    }
    columnMetadataDAO.updateColumnMetadata(all_columns_details)

  }

  def getColumnMetadata(dataset_id: String) = {
    columnMetadataDAO.getColumnMetadata(dataset_id)
  }

  def readFile(user_id: String, dataset_id: String,
               limit: Int, page: Int): Future[DatasetView] = {
    getFileMetadataById(dataset_id) flatMap { dataset =>
      previewDataAndMeta(user_id, dataset.get, limit, page)
    }
  }

  def getFileMetadataById(id: String): Future[Option[FileMetadata]] = {
    fileMetadataDAO.getFileMetadataById(id)
  }

  def previewDataAndMeta(user_id: String, dataset: FileMetadata,
                         limit: Int, page: Int): Future[DatasetView] = synchronized {
    val s3URI = filesystemService.getS3URL(dataset.parquet_file_path.get, user_id)
    val df = readDataframe(s3URI).cache()
    val number_of_rows = df.count()
    val number_of_cols = df.columns.length
    val offset = (limit * (page - 1)) - (page - 1)
    val data_frame = df.collect().toList.drop(offset).take(limit)
    val preview_data = data_frame.map(row => row.toSeq.zipWithIndex.map { case (y, i) => {
      val x = y match {
        case null => ""
        case _ => y.toString
      }
      ("_c" + i, x)
    }
    }.toMap)
    columnMetadataDAO.getColumnMetadata(dataset.id.get) map {
      columns =>
        new DatasetView(columns.toList, preview_data,
          dataset, number_of_rows, number_of_cols)
    }
  }


  def previewScoreData(meta: ScoreSummary, limit:Int, page: Int):Future[ModelView] = Future {
    val s3_url = filesystemService.getS3FUllURL(meta.score_path)
    val df = readDataframe(s3_url).cache()
    val number_of_rows = df.count()
    val number_of_cols = df.columns.length
    val offset = (limit * (page - 1)) - (page - 1)
    val data_frame = df.collect().toList.drop(offset).take(limit)
    val preview_data = data_frame.map(row => row.toSeq.zipWithIndex.map { case (y, i) => {
      val x = y match {
        case null => ""
        case _ => y.toString
      }
      ("_c" + i, x)
    }
    }.toMap)

    val list_columns: List[ColumnMetadata] = df.columns.toList.zipWithIndex map { case (c, i) =>
      new  ColumnMetadata(Some(createUUID()),c, Some(c), i, "String", "0","",false,
        2,Some(meta.dataset_id),true, false,"","", None,None, None, None, None)
    }
    new ModelView(meta.name, meta.dataset_id, meta.dataset_name,
      list_columns, preview_data, number_of_rows, number_of_cols)

  }

  def previewScore(id: Long, limit:Int, page: Int): Future[ModelView] = {
    modelScoringService.getScore(id) flatMap { score =>
      previewScoreData(score.get, limit, page)
    }
  }


  def readDataframe(uri: String): DataFrame = {
    spark.read.option("header", true).option("inferSchema", true).parquet(uri)
  }

  def getAnomalies(user_id: String,
                   dataset_id: String, page:Int,
                   limit: Int, q: String): Future[(Int, Seq[AnomalySummary])] = {

    anomalyDAO.findById(dataset_id, page, limit, q) flatMap { an =>
      getAnomalyWithCount(dataset_id,q, an)
    }

  }

  def getAnomalyWithCount(dataset_id: String, q: String,
                          an: Seq[AnomalySummary]):Future[(Int, Seq[AnomalySummary])] = {
    anomalyDAO.getAnomalyCount(dataset_id, q) map { cnt =>
      (cnt, an)
    }
  }

  def getAnomalyById(user_id: String, id: String,
                     limit: Int, page: Int): Future[AnomalyView] = {
    anomalyDAO.getAnomaly(id) flatMap { a =>
      previewAnomalyAndMeta(user_id, a.get, limit, page)
    }
  }

  def previewAnomalyAndMeta(user_id: String, meta: AnomalySummary,
                            limit: Int, page: Int): Future[AnomalyView] = synchronized {

    val list_column_id = (meta.feature_column_ids split ",").toList

    columnMetadataDAO.getColumnMetadataIn(list_column_id) map { columns =>
      val columns_names = (columns map (x => x.name)).toList
      val with_anomaly = "anomaly_score" :: columns_names
      val s3URI = filesystemService.getS3URL(meta.path.get, user_id)
      val df = readDataframe(s3URI).cache()
      val number_of_rows = df.count()
      val offset = (limit * (page - 1)) - (page - 1)
      val data_frame = df.select(with_anomaly.map(c => col(c)): _*)
        .orderBy(col("anomaly_score").desc).collect().toList.drop(offset).take(limit)

      val preview_data = data_frame.map(row => row.toSeq.zipWithIndex.map { case (y, i) => {
        val x = y match {
          case null => ""
          case _ => y.toString
        }
        ("_c" + i, x)
      }
      }.toMap)
      new AnomalyView(columns.toList, preview_data, meta, number_of_rows, (columns.toList.length + 1))
    }
  }


  def deleteAnomalyById(user_id: String, dataset_id: String, id: String): Future[Int] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val file_meta = dataset.get.copy(anomaly_count = Some(dataset.get.anomaly_count.get - 1))
      fileMetadataDAO.updateFileMeta(dataset_id, file_meta) flatMap { status =>
        anomalyDAO.delete(id)
      }
    }
  }

  def CheckDuplicateName(user_id: String,
                         request: CheckDuplicateName): Future[Boolean] = {
    fileMetadataDAO.checkDuplicate(user_id,request.dataset_id, request.name)
  }

}