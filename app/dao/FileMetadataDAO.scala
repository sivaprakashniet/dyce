package dao

import javax.inject.Inject
import entities.FileMetadata
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class FileMetadataDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class FileMetadataTable(tag: Tag)
    extends Table[FileMetadata](tag, "file_metadata") {

    def id = column[Option[String]]("id", O.PrimaryKey)

    def file_path = column[String]("file_path")

    def dataset_name = column[String]("dataset_name")

    def created_date = column[Option[String]]("created_date")

    def updated_date = column[Option[String]]("updated_date")

    def user_id = column[Option[String]]("user_id")

    def number_of_rows = column[Option[Long]]("number_of_rows")

    def file_size = column[Option[Long]]("file_size")

    def number_of_columns = column[Option[Long]]("number_of_columns")

    def parquet_file_path = column[Option[String]]("parquet_file_path")

    def download_file_path = column[Option[String]]("download_file_path")

    def dataset_status = column[Option[String]]("dataset_status")

    def decision_tree_count = column[Option[Long]]("decision_tree_count")

    def anomaly_count = column[Option[Long]]("anomaly_count")

    def gbt_count = column[Option[Long]]("gbt_count")

    def random_forest_count = column[Option[Long]]("random_forest_count")

    def model_count = column[Option[Long]]("model_count")

    def download_status = column[Option[String]]("download_status")



    override def * =
      (id, file_path, dataset_name, created_date, updated_date, user_id,
        number_of_rows, number_of_columns, file_size,
        parquet_file_path, download_file_path,  dataset_status, decision_tree_count,
        anomaly_count, gbt_count, random_forest_count, model_count,download_status) <> ((FileMetadata.apply _).tupled, FileMetadata.unapply)
  }

  implicit val metadata = TableQuery[FileMetadataTable]

  def saveFileMetadata(fileMetadata: FileMetadata): Future[String] = {
    db.run(metadata += fileMetadata) map { result =>
      fileMetadata.id.get
    }
  }

  def updateFileMetadata(dataset_id: String, file_path: String,
                         dataset_name: String, updated_date: String): Future[Int] = {
    db.run(
      metadata.filter(_.id === dataset_id).map(data => (data.file_path,
      data.dataset_name, data.updated_date))
        .update(file_path, dataset_name, Some(updated_date))
    )
  }

  def updateFileMeta(dataset_id: String,
                         fileMetadata: FileMetadata):Future[Int] = {
    db.run(metadata.filter(_.id === dataset_id).update(fileMetadata))
  }

  def getFileMetadata(user_id: String,
                      page: Int, limit: Int, q: String,
                      sort_key: String, sort_type:String): Future[Seq[FileMetadata]] = {
    val offset = (limit * (page - 1))-(page - 1)+(page-1)
    db.run {
      if(sort_key == "file_size"){
        if(sort_type =="desc"){
          metadata.filter(_.user_id === user_id)
            .filter(x =>(x.dataset_name).toLowerCase startsWith q.toLowerCase)
            .sortBy(_.file_size.desc)
            .drop(offset).take(limit).result
        }else{
          metadata.filter(_.user_id === user_id)
            .filter(x =>(x.dataset_name).toLowerCase startsWith q.toLowerCase)
            .sortBy(_.file_size.asc)
            .drop(offset).take(limit).result
        }
      }else if(sort_key == "dataset_name" ){
        if(sort_type =="desc"){
          metadata.filter(_.user_id === user_id)
            .filter(x =>(x.dataset_name).toLowerCase startsWith q.toLowerCase)
            .sortBy(_.dataset_name.desc)
            .drop(offset).take(limit).result
        }else{
          metadata.filter(_.user_id === user_id)
            .filter(x =>(x.dataset_name).toLowerCase startsWith q.toLowerCase)
            .sortBy(_.dataset_name.asc)
            .drop(offset).take(limit).result
        }
      }else{
        if(sort_type =="asc"){
          metadata.filter(_.user_id === user_id)
            .filter(x =>(x.dataset_name).toLowerCase startsWith q.toLowerCase)
            .sortBy(_.created_date.asc)
            .drop(offset).take(limit).result
        }else{
          metadata.filter(_.user_id === user_id)
            .filter(x =>(x.dataset_name).toLowerCase startsWith q.toLowerCase)
            .sortBy(_.created_date.desc)
            .drop(offset).take(limit).result
        }
      }
    }
  }

  def getFileMetadataCount(user_id: String, q: String): Future[Int] = {
    db.run(metadata.filter(_.user_id === user_id)
      .filter(x =>(x.dataset_name).toLowerCase startsWith q.toLowerCase)
      .result) map { q =>q.length}
  }

  def deleteFileMetadata(id: String): Future[Int] = {
    db.run(metadata.filter(_.id === id).delete)
  }

  def getFileMetadataById(id: String): Future[Option[FileMetadata]] = {
    db.run(metadata.filter(_.id === id).result.headOption)
  }

  def getFilemetadataStatus(id: String,

                            user_id: String,  path: String):  Future[Boolean] = {
    db.run(
      metadata.filter(_.file_path === path)
              .filter(_.user_id === user_id).filter(_.id =!= id).exists.result
    )
  }

  def updateParquetFilePath(dataset_id: String,
                            response_file_path: String,
                            status: String, rows: Long, cols: Int ):Future[Int] = {
    db.run(metadata.filter(_.id === dataset_id)
      .map(x => (x.parquet_file_path, x.dataset_status, x.download_status, x.number_of_rows, x.number_of_columns))
      .update(Some(response_file_path), Some(status), Some("MODIFIED"), Some(rows), Some(cols.toLong))
      .map(_.toInt))
  }

  def updateDownloadPath(id: Option[String],
                         download_dataset_path: String):Future[Int] = {
    db.run(metadata.filter(_.id === id)
      .map(x => (x.download_file_path, x.download_status))
      .update(Some(download_dataset_path), Some("FINISHED"))
      .map(_.toInt))
  }

  def checkDuplicate(user_id: String,
                     dataset_id: Option[String], name: String): Future[Boolean] = {

    if(dataset_id == None)
      db.run(metadata.filter(_.user_id === user_id)
      .filter(_.dataset_name === name).exists.result)
    else
      db.run(metadata.filter(_.user_id === user_id)
        .filter(_.dataset_name === name).filter(_.id =!= dataset_id.get).exists.result)
  }

}