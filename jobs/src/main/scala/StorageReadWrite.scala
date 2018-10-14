import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, S3Object}
import play.api.libs.json.Json

import scala.io.Source

object StorageReadWrite {
  val bucket_name_for_results = S3Config.bucket_name_for_results
  val access_key = S3Config.access_key
  val secret_key = S3Config.secret_key

  def saveResponse(response_filepath: String, response_body: String) = {
    S3Config.s3client.putObject(new PutObjectRequest(bucket_name_for_results, response_filepath,
      new ByteArrayInputStream(response_body.getBytes()),
      new ObjectMetadata()))
  }

  def getRequest(request_filepath: String) = {
    val fileObject: S3Object = S3Config.s3client.getObject(bucket_name_for_results, request_filepath)
    val source = Source.fromInputStream(fileObject.getObjectContent)
    val request_body = Json.parse(source.mkString)
    request_body
  }

}
