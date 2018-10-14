package job

import javax.inject.Inject
import controllers.ControllerImplicits._
import entities._
import dao.{ColumnMetadataDAO, FileMetadataDAO, JobDAO}
import filesystem.{FilesystemService, S3Config}
import play.api.Configuration
import play.api.libs.json.{JsObject, JsValue, Json}
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.BSONObjectID

// For MongoDB
import reactivemongo.api.Cursor
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.play.json.collection.JSONCollection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class PipelineService @Inject()(val reactiveMongoApi: ReactiveMongoApi, s3Config: S3Config,
                                jobResult: JobResult, filesystemService: FilesystemService,
                                configuration: Configuration, fileMetadataDAO: FileMetadataDAO,
                                columnMetadataDAO: ColumnMetadataDAO, jobDAO: JobDAO) {



  import play.modules.reactivemongo.json._

  lazy val db = reactiveMongoApi.db
  lazy val univariate_collection = db.collection[JSONCollection]("univariate")
  def currentDateTime = System.currentTimeMillis()





  def preProcessDataset(user_id: String,
                        dataset_id: String): Future[(String, PreProcessMetaData, Long)] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val new_job = new Job(0, user_id, dataset_id, dataset.get.dataset_name, "Pre - Processing",
        currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
      jobDAO.create(new_job) flatMap { job =>
        getFileAndColumnMeta(dataset_id).map { pre_process_meta =>
          (user_id, pre_process_meta, job.job_id)
        }
      }
    }
  }

  def createFromQuery(user_id: String, meta: Option[IngestParameters]):Future[Long] = {
      val new_job = new Job(0, user_id,"", meta.get.name.getOrElse(""), "Creating Dataset",
        currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
        jobDAO.create(new_job) map (job => { job.job_id
        })
  }


  def getFileAndColumnMeta(dataset_id: String): Future[PreProcessMetaData] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { file_meta =>
      columnMetadataDAO.getColumnMetadata(dataset_id) map { columns =>
        new PreProcessMetaData(file_meta.get, columns.toList)
      }
    }
  }


  def computeUnivariate(user_id: String,
                        dataset_id: String): Future[(String, UnivariateMeta, Long)] =
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>

      val new_job = new Job(0, user_id, dataset_id,dataset.get.dataset_name, "Univariate",
        currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))

    jobDAO.create(new_job) flatMap { job =>
        columnMetadataDAO.getColumnMetadata(dataset_id) map { cmeta =>
          val columns: List[Columns] = cmeta.toList map (x =>
            new Columns(x.id.get, x.name,x.position.toInt, x.datatype, 12, x.decimal.toInt)
            )
          val meta = new UnivariateMeta(dataset_id,
            dataset.get.parquet_file_path.get, dataset.get.dataset_name, columns)
          (user_id, meta, job.job_id)
        }
      }
    }

  def constructUnivariateMeta(dataset: FileMetadata,
                              columns: List[ColumnMetadata]): Future[(UnivariateMeta, Long)] = {
    val new_job = new Job(0, dataset.user_id.get, dataset.id.get,dataset.dataset_name, "Univariate",
      currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
    fileMetadataDAO.getFileMetadataById(dataset.id.get) flatMap { dataset =>
      jobDAO.create(new_job) map { job =>
        val cols: List[Columns] = columns.toList map (x =>
          new Columns(x.id.get, x.name, x.position.toInt, x.datatype, 12, x.decimal.toInt)
          )
        (new UnivariateMeta(dataset.get.id.get,
          dataset.get.parquet_file_path.get, dataset.get.dataset_name, cols), job.job_id)
      }
    }
  }

  def saveRequestDB(collection: JSONCollection,
                    request: JsObject): Future[WriteResult] = collection.insert(request)

  def computeBivariate(user_id: String,
                       dataset_id: String): Future[(String, Option[String], Long, JsValue)] = {
    val request_json_id = BSONObjectID.generate().stringify
    columnDetails(dataset_id) flatMap { cols =>

      val columns = cols map (c => (c \ "column_id").get.as[String])

      var pairs: List[BivariatePair] = List()
      for (a <- columns; b <- columns) yield
        if (!pairs.exists(p => (p.column_1_id == a) && (p.column_2_id == b) ||
          (p.column_1_id == b) && (p.column_2_id == a))) {
          pairs = pairs :+ new BivariatePair(a, b)
        }

      fileMetadataDAO.getFileMetadataById(dataset_id) flatMap (
        dataset => {
          val dataset_name = dataset.get.dataset_name
          val dataset_path = dataset.get.parquet_file_path
          val bivariateRequest = new BivariateRequest(request_json_id, dataset_id, dataset_name, cols, pairs)
          val new_job = new Job(0, user_id, dataset_id, dataset.get.dataset_name, "Bivariate",
            currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
          jobDAO.create(new_job) map (job => {
            (user_id, dataset_path, job.job_id, Json.toJson(bivariateRequest))
          })
        })
    }
  }

  def constructBivariateMeta(dataset: FileMetadata,
                             columns: List[ColumnMetadata]): Future[(String, Option[String], Long, JsValue)] = {

    val col_ids: List[String] = columns map (x => x.id.get)
    selectedColumnDetails(dataset.id.get, col_ids) flatMap { selected_cols =>
      val select_columns = selected_cols map (c => (c \ "column_id").get.as[String])
      columnDetails(dataset.id.get) flatMap { cols =>

        val columns = cols map (c => (c \ "column_id").get.as[String])
        val total_columns = columns ++ select_columns
        val total_cols = cols ++ selected_cols
        var pairs: List[BivariatePair] = List()
        for (a <- total_columns; b <- select_columns) yield
          if (!pairs.exists(p => (p.column_1_id == a) && (p.column_2_id == b) ||
            (p.column_1_id == b) && (p.column_2_id == a))) {
            pairs = pairs :+ new BivariatePair(a, b)
          }
        val bivariateRequest = new BivariateRequest("", dataset.id.get, dataset.dataset_name, total_cols, pairs)
        val new_job = new Job(0, dataset.user_id.get, dataset.id.get, dataset.dataset_name, "Bivariate",
          currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
        fileMetadataDAO.getFileMetadataById(dataset.id.get) flatMap { dataset =>
          jobDAO.create(new_job) map (job => {
            (dataset.get.user_id.get, dataset.get.parquet_file_path, job.job_id, Json.toJson(bivariateRequest))
          })
        }
      }
    }
  }

  def columnDetails(dataset_id: String): Future[List[JsObject]] = {
    val cursor: Cursor[JsObject] = univariate_collection
      .find(Json.obj("dataset_id" -> dataset_id)).cursor[JsObject]
    val futureUnivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureUnivariateList map (res =>
      res map (x => x - ("dataset_id") - ("_id") - ("dataset_name") - ("missing") - ("histogram"))
      )
  }

  def selectedColumnDetails(dataset_id: String,
                            column_ids: List[String]): Future[List[JsObject]] = {
    val cursor: Cursor[JsObject] = univariate_collection
      .find(Json.obj("dataset_id" -> dataset_id, "column_id" -> Json.obj("$in" -> column_ids)))
      .cursor[JsObject]
    val futureUnivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureUnivariateList map (res =>
      res map (x => x - ("dataset_id") - ("_id") - ("dataset_name") - ("missing") - ("histogram"))
      )
  }

}