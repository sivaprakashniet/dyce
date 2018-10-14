package filesystem

import javax.inject.Inject

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.auth.AWSStaticCredentialsProvider
import play.api.Configuration

class S3Config @Inject()(configuration: Configuration) {

  val access_key = configuration.underlying.getString("s3config.access_key")
  val secret_key = configuration.underlying.getString("s3config.secret_key")
  val region = configuration.underlying.getString("s3config.region")

  val bucket_name = configuration.underlying.getString("s3config.bucket_name")
  val result_bucket_name = configuration.underlying.getString("s3config.bucket_name_for_job_results")
  val expiration_time = configuration.underlying.getString("s3config.expiration_time")

  val awsCreds = new BasicAWSCredentials(access_key, secret_key);
  val s3client = AmazonS3ClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
    .withRegion(region)
    .build()
}