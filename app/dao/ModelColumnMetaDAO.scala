package dao

import javax.inject.Inject

import entities.ModelColumnMetadata
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ModelColumnMetaDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class ModelColumnMetadataTable(tag: Tag)
    extends Table[ModelColumnMetadata](tag, "model_column_meta") {

    def id = column[Option[String]]("id", O.PrimaryKey)

    def model_meta_id = column[String]("model_meta_id")

    def name = column[String]("name")

    def description = column[Option[String]]("description")

    def position = column[Int]("position")

    def datatype = column[String]("datatype")

    def format = column[String]("format")

    def separator = column[Boolean]("separator")

    def decimal = column[Long]("decimal")

    def dataset_id = column[Option[String]]("dataset_id")

    def calculated = column[Boolean]("calculated")

    def is_dv = column[Boolean]("is_dv")


    override def * =
      (id, model_meta_id, name, description, position, datatype, format, separator,
        decimal, dataset_id, calculated, is_dv) <> (
        (ModelColumnMetadata.apply _).tupled, ModelColumnMetadata.unapply)
  }

  implicit val metadata = TableQuery[ModelColumnMetadataTable]

  def saveColumnMetadata(column_details: List[ModelColumnMetadata]): Future[Option[Int]] = {
    db.run(metadata ++= column_details)
  }

  def getModelColumnMetadata(model_id: String, status: Boolean): Future[Seq[ModelColumnMetadata]] = {
    db.run(metadata.filter(_.model_meta_id === model_id)
      .filter(_.is_dv === status).sortBy(_.position.asc).result)
  }


}