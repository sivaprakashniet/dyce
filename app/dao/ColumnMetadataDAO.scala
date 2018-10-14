package dao

import javax.inject.Inject

import entities.ColumnMetadata
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ColumnMetadataDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class ColumnMetadataTable(tag: Tag)
    extends Table[ColumnMetadata](tag, "column_metadata") {

    def id = column[Option[String]]("id", O.PrimaryKey)

    def name = column[String]("name")

    def description = column[Option[String]]("description")

    def position = column[Long]("position")

    def datatype = column[String]("datatype")

    def format = column[String]("format")

    def display_format = column[String]("display_format")

    def separator = column[Boolean]("separator")

    def decimal = column[Long]("decimal")

    def dataset_id = column[Option[String]]("dataset_id")

    def visibility = column[Boolean]("visibility")

    def calculated = column[Boolean]("calculated")

    def formula = column[String]("formula")

    def metrics = column[String]("metrics")

    def created_date = column[Option[String]]("created_date")

    def modified_date = column[Option[String]]("modified_date")

    def new_name = column[Option[String]]("new_name")

    def status = column[Option[String]]("status")

    def raw_formula = column[Option[String]]("raw_formula")

    override def * =
      (id, name, description, position, datatype, format, display_format, separator, decimal, dataset_id,
        visibility, calculated, formula, metrics,
        created_date, modified_date, new_name, status, raw_formula) <> (
        (ColumnMetadata.apply _).tupled, ColumnMetadata.unapply)
  }

  implicit val metadata = TableQuery[ColumnMetadataTable]

  def saveColumnMetadata(column_details: List[ColumnMetadata]): Future[Option[Int]] = {
    db.run(metadata ++= column_details)
  }

  def updateColumnMetadata(column_details: List[ColumnMetadata]): Future[List[Int]] = {
    Future.sequence {
      column_details map { c =>
        db.run(metadata.filter(_.id === c.id).update(c))
      }
    }
  }


  def deleteColumnMetadataByDataset(dataset_id: String):Future[Int] = {
    db.run(metadata.filter(_.dataset_id === dataset_id).delete)

  }

  def ignoreColumnMetadataByDataset(dataset_id: String):Future[Int] = {
    db.run(metadata.filter(_.dataset_id === dataset_id)
      .filter(_.visibility===true).delete)
  }

  def deleteColumnMetadata(column_details: List[ColumnMetadata]): Future[List[Int]] = {
    Future.sequence {
      column_details map { c =>
        db.run(metadata.filter(_.id === c.id).delete)
      }
    }
  }

  def getColumnMetadata(dataset_id: String): Future[Seq[ColumnMetadata]] = {
    db.run(metadata.filter(_.dataset_id === dataset_id).sortBy(_.position.asc).result)
  }

  def getColumnMetadataIn(list_column_id: List[String]):Future[Seq[ColumnMetadata]] = {
    db.run(metadata.filter(_.id inSet list_column_id)
      .sortBy(_.position.asc).result)
  }
  def getColumnMetadataInName(list_column_names: List[String],
                              dataset_id: String):Future[Seq[ColumnMetadata]] = {
    db.run(metadata.filter(_.dataset_id === dataset_id).filter(_.name inSet list_column_names)
      .sortBy(_.position.asc).result)
  }
}