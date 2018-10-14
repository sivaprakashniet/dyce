package services


import javax.inject._

import dao.{FileMetadataDAO, ModelColumnMetaDAO, ModelDAO, ScoreDAO}
import entities._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

class ModelScoringService @Inject()(modelDAO: ModelDAO,
                                    scoreDAO: ScoreDAO,
                                    modelColumnMetaDAO: ModelColumnMetaDAO,
                                    fileMetadataDAO: FileMetadataDAO) {


  def deleteByDatasetId(dataset_id: String): Future[Int] = {
      modelDAO.deleteByDatasetId(dataset_id)
  }

  def getDV(list:Seq[ModelColumnMetadata],
            m: ModelMeta):Future[(Seq[ModelColumnMetadata], ModelColumnMetadata)] = {
    modelColumnMetaDAO.getModelColumnMetadata(m.id.get, true) map { col =>
      (list, col(0))
    }
  }

  def getColumns(m: ModelMeta): Future[(Seq[ModelColumnMetadata], ModelColumnMetadata)] ={
    modelColumnMetaDAO.getModelColumnMetadata(m.id.get, false) flatMap { input_cols =>
      getDV(input_cols, m)
    }
  }

  def getModelSchema(meta: Seq[ModelMeta]):Future[Seq[ModelMetaSchema]] = Future.sequence{
    meta.map { m => {
        getColumns(m) map { res =>
          new ModelMetaSchema(m, res._1.toList, res._2)
        }
      }
    }
  }

  def getModels(dataset_id: String, page_no: Int,
                limit: Int, q: String ): Future[Seq[ModelMeta]] =  {

    modelDAO.findById(dataset_id, page_no, limit, q)
  }



  def findById(id: String): Future[Option[ModelMeta]] = {
    modelDAO.get(id)
  }

  def findByUserID(user_id: String, page_no: Option[Int],
                   limit: Option[Int]): Future[(Int, Seq[ModelMetaSchema])] = {
    modelDAO.getCount(user_id) flatMap { c =>
      modelDAO.findByUserID(user_id, page_no, limit) flatMap { m =>
        getModelSchema(m) map { l =>
          (c, l)
        }
      }
    }
  }

  def modelWithCount(user_id: String,
                     m:Seq[ModelMeta]):Future[(Int, Seq[ModelMeta])] = {
    modelDAO.getCount(user_id) map { c =>
      (c, m)
    }
  }

  def deleteById(user_id: String, id: String): Future[Int] = {
      modelDAO.delete(id)
  }


  def scoreWithCount(dataset_id: String,
                     s: Seq[ScoreSummary],q:Option[String]):Future[(Int, Seq[ScoreSummary])]= {
    scoreDAO.getScoreCount(dataset_id, q) map { c =>
      (c, s)
    }
  }
  def getScores(dataset_id: String, page_no: Option[Int],
                limit: Option[Int], q:Option[String]):Future[(Int, Seq[ScoreSummary])] = {
    scoreDAO.getScores(dataset_id, page_no, limit, q) flatMap {  s =>
      scoreWithCount(dataset_id, s, q)
    }
  }


  def getScore(id:Long):Future[Option[ScoreSummary]] = {
    scoreDAO.getScore(id)
  }

  def deleteScoreById(id: Long, dataset_id: String): Future[Int] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val file_meta = dataset.get.copy(model_count = Some(dataset.get.model_count.get - 1))
      fileMetadataDAO.updateFileMeta(dataset_id, file_meta) flatMap { status =>
        scoreDAO.delete(id)
      }
    }
  }

  def checkModelName(user_id: String,
                     dataset_id: String, u: CheckDuplidateModelName):Future[Boolean] = {

    modelDAO.checkCheckDuplicate(user_id,dataset_id,u)
  }

}