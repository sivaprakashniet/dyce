package services


import javax.inject._

import dao.{FileMetadataDAO, ModelDAO, ScoreDAO}
import entities._
import org.apache.spark.sql.DataFrame
import filesystem.FilesystemService

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import services.SparkConfig._


class DataPreparationService @Inject()( modelDAO: ModelDAO,
                                        scoreDAO: ScoreDAO,
                                        filesystemService: FilesystemService,
                                        fileMetadataDAO: FileMetadataDAO) {


  //def consturctSchema(): List[ColumnMetadata]

  def readDataframe(uri: String, name: String) = spark.read.option("header", true).option("inferSchema", true)
      .option("nullValue", "NA").parquet(uri).createTempView(name)

  def constructTable(dataset_lists: List[String]) = {
    dataset_lists.map { id =>
      fileMetadataDAO.getFileMetadataById(id) map { d =>
        val s3URI = filesystemService.getS3URL(d.get.parquet_file_path.get, d.get.user_id.get)
        readDataframe(s3URI, d.get.dataset_name)
      }
    }
  }

  def prepareDataset(user_id: String, meta: PrepareDataset):Future[Either[String, DataView]] = Future {
    val x  = constructTable(meta.dataset_ids)
    val df = spark.sql(meta.query)
    try {
      val list_columns: List[ColumnMetadata] = df.columns.toList.zipWithIndex map { case (c, i) =>
        new  ColumnMetadata(None,c, Some(c), i, "String","", "",false,
          2,None,true, false,"","", None,None, None, None, None)
      }
      val data_frame = df.collect().toList.take(20)

      val preview_data = data_frame.map(row => row.toSeq.zipWithIndex.map { case (y, i) => {
        val x = y match {
          case null => "NA"
          case _ => y.toString
        }
        ("_c" + i, x)
      }
      }.toMap)
        Right(new DataView(list_columns, preview_data))
      } catch {
      case e: Exception =>{
        println(e)
        Left(e.toString)
      }
    }
  }

}
