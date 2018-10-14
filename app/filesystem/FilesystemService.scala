package filesystem

import java.security.MessageDigest
import javax.inject.Inject

import com.amazonaws.services.s3.model.{GeneratePresignedUrlRequest, S3Object, S3ObjectSummary}
import com.amazonaws.{AmazonClientException, AmazonServiceException, HttpMethod}
import dao.FileMetadataDAO

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try
import scala.io.{BufferedSource, Source}

class FilesystemService @Inject()(s3config: S3Config,
                                  fileMetadataDAO: FileMetadataDAO) {

  type URL = String
  type Error = Map[String, String]
  val s3client = s3config.s3client
  val expiration_time = s3config.expiration_time
  val bucket_name = s3config.bucket_name
  val result_bucket_name = s3config.result_bucket_name

  //gets presigned url
  def generatePresignedUrlForUpload(user_id: String,
                                    filename: String): Future[Either[Error, URL]] = Future {

    val foldername = getFolderName(user_id)

    val response = Try {
      val object_key = foldername + filename
      val expiration = generateExpirationTime(expiration_time.toInt)
      val generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucket_name, object_key)

      generatePresignedUrlRequest.setMethod(HttpMethod.PUT)
      generatePresignedUrlRequest.setExpiration(expiration)

      val url = s3client.generatePresignedUrl(generatePresignedUrlRequest)

      Right(url.toString())

    }

    (response recover {
      case exception: AmazonServiceException => {
        val error_map = Map("error code" -> "403",
          "exception" -> "AmazonServiceException",
          "description" -> ("Caught an AmazonServiceException,"
            + " which means your request made it"
            + " to Amazon S3, but was rejected with an error response"
            + " for some reason."),
          "error message" -> exception.getMessage().toString,
          "http code" -> exception.getStatusCode().toString,
          "aws error code" -> exception.getErrorCode().toString,
          "error type" -> exception.getErrorType().toString,
          "request id" -> exception.getRequestId().toString)

        Left(error_map)

      }

      case ace: AmazonClientException => {
        val error_map = Map("error code" -> "500",
          "exception" -> "AmazonClientException",
          "description" -> ("Caught an AmazonClientException," +
            " which means the client encountered" +
            " an internal error while trying to communicate" +
            " with S3," +
            " such as not being able to access the network."),
          "error message" -> ace.getMessage().toString)

        Left(error_map)

      }
    }).get
  }

  def generatePresignedUrlForDownload(user_id: String,
                                      dataset_id: String): Future[Either[Error, URL]] = {

    val foldername = getFolderName(user_id)

    fileMetadataDAO.getFileMetadataById(dataset_id) map { res =>
      val filename = res.get.download_file_path.get
      val object_key = foldername + filename
      val expiration = generateExpirationTime(expiration_time.toInt)
      val generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucket_name, object_key)

      generatePresignedUrlRequest.setMethod(HttpMethod.GET)
      generatePresignedUrlRequest.setExpiration(expiration)

      val url = s3client.generatePresignedUrl(generatePresignedUrlRequest)

      Right(url.toString())
    }

  }

  //generates expiration time for presigned url
  def generateExpirationTime(duration: Int) = {
    val expiration = new java.util.Date()
    var milliSeconds = expiration.getTime()
    milliSeconds = milliSeconds + duration
    expiration.setTime(milliSeconds)
    expiration
  }

  def getFolderName(userid: String): String = {
    def encrypt_with_md5(text: String): String = {
      val encrypted_string = MessageDigest.getInstance("MD5").digest(text.getBytes)
        .map(0xFF & _).map {
        "%02x".format(_)
      }.foldLeft("") {
        _ + _
      }
      encrypted_string
    }

    val encrypted_value = encrypt_with_md5(userid)
    encrypted_value.toString
  }

  def writeFile(key: String, str: String): Future[Option[Boolean]] = {
    Future.successful(
    try {
      val x = s3client.putObject(bucket_name, key, str)
      Some(true)
    } catch {
      case e: Exception => {
        Some(false)
      }
    }
    )
  }

  def readJson(key: String):Future[Option[String]] = {
    Future.successful(
      try{
        val fileObject: S3Object = s3client.getObject(result_bucket_name, key)
        val source: BufferedSource = Source.fromInputStream(fileObject.getObjectContent)
        Some(source.mkString)
      }catch {
        case e: Exception => {
          None
        }
      }
    )
  }

  //File service layer
  def deleteFileOrFolder(fullFileOrFolderPath: String, user_id: String): Option[Boolean] = {
    val key = getFolderName(user_id) + fullFileOrFolderPath
    deleteFromS3(key)
  }

  def deleteFromS3(key: String): Option[Boolean] = {
    try {
      if (checkInS3(key).get) {
        val x = s3client.deleteObject(bucket_name, key)
        Some(true)
      } else Some(false)

    } catch {
      case e: Exception => {
        Some(false)
      }
    }
  }

  def checkInS3(key: String): Option[Boolean] = {
    try {
      val fs_object = s3client.getObject(bucket_name, key)
      if (fs_object != None) Some(true) else Some(false)
    } catch {
      case e: Exception => {
        Some(false)
      }
    }
  }

  def checkInResultS3(key: String): Option[Boolean] = {
    try {
      val fs_object = s3client.getObject(result_bucket_name, key)
      if (fs_object != None) Some(true) else Some(false)
    } catch {
      case e: Exception => {
        Some(false)
      }
    }
  }

  def getS3URL(path: String,
               user_id: String): String = "s3a://"+ bucket_name +"/"+getFolderName(user_id)+ path

  def getS3FUllURL(path: String): String = "s3a://"+ bucket_name +"/"+ path

  def getS3ParquetLocation(user_id: String): String = getS3URL("/parquet_datasets/", user_id)


  def renameFileOrFolder(oldPath: String,
                         newPath: String, user_id: String): Option[Boolean] = {
    val fromkey = getFolderName(user_id) + oldPath
    val tokey = getFolderName(user_id) + newPath

    renameInS3(fromkey, tokey)
  }


  def copyFileOrFolder(oldPath: String,
                         newPath: String, user_id: String, new_user_id: String): Option[Boolean] = {
    val fromkey = getFolderName(user_id) + oldPath
    val tokey = getFolderName(new_user_id) + newPath
    copyInS3(getFolderName(user_id), fromkey, getFolderName(new_user_id), tokey)
  }




  def renameInS3(from: String, to: String): Option[Boolean] = {
    try {
      if (!checkInS3(to).get) {
        val file_mv = s3client.copyObject(bucket_name, from, bucket_name, to)
        deleteFromS3(from)
      } else Some(false)
    } catch {
      case e: Exception => {
        println(e)
        Some(false)
      }
    }
  }

  def copyInS3(from_user:String, from: String, touser: String, to: String): Option[Boolean] = {
    try {
      val list_objs = s3client.listObjects(bucket_name, from).getObjectSummaries

      val objs = list_objs.toArray.toList.map { x =>
        val to_path = (x.asInstanceOf[S3ObjectSummary].getKey).replace(from_user, touser)
        s3client.copyObject(bucket_name, x.asInstanceOf[S3ObjectSummary].getKey, bucket_name, to_path)
      }
      Some(true)
    } catch {
      case e: Exception => {
        Some(false)
      }
    }
  }

  def listFiles(in: String, out_path: String):Future[Array[Boolean]] = Future {
    val list_objs = s3client.listObjects(bucket_name, in).getObjectSummaries()
    list_objs.toArray.map { x =>
      val key = x.asInstanceOf[S3ObjectSummary].getKey
      if(key.endsWith(".csv")){
        s3client.copyObject(bucket_name, key, bucket_name, out_path)
        true
      }else  false
    }
  }

}